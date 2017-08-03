"""
Provides extractions using the `refextract
<https://github.com/inspirehep/refextract>`_ package.
"""

from reflink.services import RefExtract
from reflink import logging
logger = logging.getLogger(__name__)


FIELD_MAPPINGS = [      # Maps refextract field names to our field names.
    ('doi', 'doi'),
    ('journal_page', 'pages'),
    ('raw_ref', 'raw'),
    ('journal_title', 'title'),
    ('journal_volume', 'volume'),
    ('journal_issue', 'issue'),
    ('title', 'title'),
    ('year', 'year')
]


def _transform(refextract_metadatum):
    metadatum = {'reftype': 'citation'}
    for re_key, key in FIELD_MAPPINGS:
        value = refextract_metadatum.get(re_key)
        if value:
            metadatum[key] = value[0]   # All refextract values are lists.
    if 'author' in refextract_metadatum:
        metadatum['authors'] = [{'fullname': author} for author
                                in refextract_metadatum['author']]
    return metadatum


def extract_references(filename: str, document_id: str) -> str:
    """
    Wrapper for :func:`refextract.extract_references_from_file` function.

    Parameters
    ----------
    filename : str
        Name of the pdf from which to extract references.

    Returns
    -------
    references : list of dicts
        Reference metadata extracted from PDF.
    """
    refextract = RefExtract()
    try:
        return [_transform(reference) for reference
                in refextract.session.extract_references(filename)]
    except IOError as e:
        raise IOError('%s' % e) from e
    except Exception as e:
        msg = 'Failed to extract references from %s: %s' % (filename, e)
        raise RuntimeError(msg) from e
