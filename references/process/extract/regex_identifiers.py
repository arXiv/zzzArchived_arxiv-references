from references.domain import ExtractedReference, Author, Identifier

import regex as re

from references.process.extract.regex_arxiv import REGEX_ARXIV_FLEXIBLE

# https://stackoverflow.com/questions/27910/finding-a-doi-in-a-document-or-page
REGEX_DOI = (
    r'(?:'                      # optional prefix / url
      r'(?:doi\:(?://)?)'         # doi:(//)
        r'|'                        # OR
      r'(?:http[s]?\://'          # http[s]://
      r'(?:dx\.)?doi\.org\/)'     # (dx.)doi.org/
    r')?'                       # end optional

    r'('                        # begin match
      r'10[.]'                    # directory indicator is 10.
      r'[0-9]{3,}'                # registrant code, numeric 3+ digits
      r'(?:[.][0-9]+)*'           # registrant sub-element
      r'/(?:(?!["&\'#%])\S)+'     # actual document identifier now (#% not STD)
    r')'                        # end match
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


def longest_string(strings):
    """Return the longest string from the bunch."""
    index, value = max(enumerate(strings), key=lambda x: len(x[1]))
    return value


def extract_identifiers(text):
    """
    Get available ID metadata from a text selection.

    Parameters
    ----------
    text : str
        Raw text from which to extract arXiv ids or DOIs

    Returns
    -------
    metadata : :class:`ExtractedReference`
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
    document = {}
    arxivids = [longest_string(ID) for ID
                in re.findall(REGEX_ARXIV_FLEXIBLE, text)]
    if arxivids:
        if len(arxivids) > 1:
            document['arxiv_id'] = arxivids
        else:
            document['arxiv_id'] = arxivids[0]

    dois = re.findall(REGEX_DOI, text)
    if dois:
        document['doi'] = dois[0]

    isbn10 = re.findall(REGEX_ISBN_10, text)
    isbn13 = re.findall(REGEX_ISBN_13, text)

    # gather the identifiers one at a time
    identifiers = []
    if isbn10:
        identifiers.extend([
            Identifier(identifier_type='ISBN', identifier=ID)
            for ID in isbn10
        ])

    if isbn13:
        identifiers.extend([
            Identifier(identifier_type='ISBN', identifier=ID)
            for ID in isbn13
        ])

    if identifiers:
        document['identifiers'] = identifiers

    return ExtractedReference(**document)
