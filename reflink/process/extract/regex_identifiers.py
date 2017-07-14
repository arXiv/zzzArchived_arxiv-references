import re

# FIXME -- remove bib walking parts from latexinjector and make more general
# https://stackoverflow.com/questions/27910/finding-a-doi-in-a-document-or-page
from reflink.process.inject import latexinjector

categories = [
    "acc-phys", "adap-org", "alg-geom", "ao-sci", "astro-ph", "atom-ph",
    "bayes-an", "chao-dyn", "chem-ph", "cmp-lg", "comp-gas", "cond-mat", "cs",
    "dg-ga", "funct-an", "gr-qc", "hep-ex", "hep-lat", "hep-ph", "hep-th",
    "math", "math-ph", "mtrl-th", "nlin", "nucl-ex", "nucl-th", "patt-sol",
    "physics", "plasm-ph", "q-alg", "q-bio", "quant-ph", "solv-int", "supr-con" 
]
_c = r'|'.join(categories)

# this one does not include the mixed form cs.AI/1204.0123 but correctly gets
# either pure old (cs/0123456) or pure new (arXiv:1702.01235). also allows
# pure number format 1204.1234:
#       cs/0123456 arXiv:1702.01235 1204.1234
REGEX_ARXIV = (
    r'(?i:arxiv[\:\/])?'    # optional prefix of arxiv: | arXiv(:|/)

    r'('                    # begin match
      r'(?:'                  # id vs version
        r'(?:'+_c+r')\/\d{7}'   # IDs of the form hep-th/0123456
          r'|'                    # OR
        r'(?:\d{4}\.\d{4,5})'   # IDs of the form 1703.04422
      r')'
      r'(?:v\d+)?'            # optional version number
    r')'                    # close match
)

# this one rejects number only 1204.0123 but does allow for pure new, pure old,
# as well as mixed formats. due to the need to strip parts of the matched
# format, there are two groups returned, (old match, new match)
#       cs/0123456 arXiv:1702.01235 cs.AI/1204.0123
REGEX_ARXIV_FULL = (
    r'\b(?:'
      r'(?:'
        r'(?:'                    # IDs that include forms like 1010.01234
          r'(?i:arxiv[:/])'         # prefix of arxiv: | arXiv(:/)
            r'|'                      # OR
          r'(?:'                    # prefix of cat.MIN/
            r'(?:'+_c+r')'            # category (cs, math)
            r'(?:[.][A-Z]{2})?/'      # subcategory (NT, AI)
          r')'
        r')'
        r'('
          r'\d{4}[.]\d{4,5}'        # numerical identifier
          r'(?:v\d+)?'              # version number
        r')'
      r')'
    r'|'                            # OR 
      r'('
        r'(?:'+_c+r')'            # category identifier (cs, math)
        r'(?:[.][A-Z]{2})?/'      # optional minor category (AI, NT)
        r'\d{7}'                  # 7 digit identifier
        r'(?:v\d+)?'              # version number
      r')'
    r')\b'
)

REGEX_DOI = (
    r'(?:'                  # optional prefix / url
      r'(?:doi\://)'          # doi://
        r'|'                    # OR
      r'(?:http[s]?\://(?:dx\.)?doi\.org\/)' # http[s]://(dx.)doi.org
    r')?'                   # end optional

    r'('                    # begin match
      r'10[.]'                # directory indicator is 10.
      r'[0-9]{3,}'            # registrant code, numeric 3+ digits
      r'(?:[.][0-9]+)*'       # registrant sub-element
      r'/(?:(?!["&\'#%])\S)+' # actual document identifier now (#% not formal)
    r')'                    # end match
)

REGEX_ISBN_10 = (
    r'\b(?:ISBN(?:-10)?:?\ )?'
    r'(?=[0-9X]{10}$|(?=(?:[0-9]+[-\ ]){3})[-\ 0-9X]{13}$)'
    r'[0-9]{1,5}[-\ ]?[0-9]+[-\ ]?[0-9]+[-\ ]?[0-9X]\b'
)
REGEX_ISBN_13 = (
    r'\b(?:ISBN(?:-13)?:?\ )?'
    r'(?=[0-9]{13}$|(?=(?:[0-9]+[-\ ]){4})[-\ 0-9]{17}$)'
    r'97[89][-\ ]?[0-9]{1,5}[-\ ]?[0-9]+[-\ ]?[0-9]+[-\ ]?[0-9]\b'
)

def extract_identifiers(text):
    """
    Get available ID metadata from a text selection.

    Parameters
    ----------
    text : str
        Raw text from which to extract arXiv ids or DOIs

    Returns
    -------
    metadata : dictionary

        The metadata dictionary corresponding to what was found,
        see schema for formatting specifics. Generally, will be similar to:

            {
                'doi': '10.1000/xyz123',
                'identifiers': [
                    {
                        'identifier_type': 'arxiv',
                        'identifier': 'hep-th/0123456'
                    }
                ]
            }
    """
    arxivids = re.findall(REGEX_ARXIV_FULL, text)
    dois = re.findall(REGEX_DOI, text)

    arxivdoc = {}
    doidoc = {}

    if arxivids:
        arxivdoc = {
            'identifiers': [
                {'identifier_type': 'arxiv', 'identifier': (ID[0] or ID[1])}
                for ID in arxivids
            ]
        }
    if dois:
        doidoc = {
            'doi': dois[0]
        }

    return dict(doidoc, **arxivdoc)
