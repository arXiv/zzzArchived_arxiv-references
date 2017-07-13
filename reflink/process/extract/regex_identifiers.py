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
    arxiv_match = re.match(REGEX_ARXIV, text)
    doi_match = re.match(REGEX_DOI, text)

    arxivdoc = {}
    doidoc = {}

    if arxiv_match:
        arxivid = arxiv_match.groups()[0]
        arxivdoc = {
            'identifiers': [{
                'identifier_type': 'arxiv',
                'identifier': arxivid
             }]
        }
    if doi_match:
        doi = doi_match.groups()[0]
        doidoc = {
            'doi': doi
        }

    return dict(doidoc, **arxivdoc)
