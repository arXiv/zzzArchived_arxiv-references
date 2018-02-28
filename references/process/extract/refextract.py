"""
Provides extractions using the refextract package.

See <https://github.com/inspirehep/refextract>.
"""

from typing import List, Any, Dict

from references.domain import ExtractedReference, Identifier, Author
from references.services import refextract
from references import logging
logger = logging.getLogger(__name__)


FIELD_MAPPINGS = [      # Maps refextract field names to our field names.
    ('doi', 'doi'),
    ('journal_page', 'pages'),
    ('raw_ref', 'raw'),
    ('journal_title', 'source'),
    ('journal_volume', 'volume'),
    ('journal_issue', 'issue'),
    ('title', 'title'),
    ('year', 'year')
]


def _transform(refextract_metadatum: dict) -> ExtractedReference:
    """
    Restructure refextract output to match internal extraction struct.

    Parameters
    ----------
    refextract_metadatum : dict
        RefExtract output.

    Returns
    -------
    dict
    """
    metadatum: Dict[str, Any] = {'reftype': 'citation'}
    for re_key, key in FIELD_MAPPINGS:
        value = refextract_metadatum.get(re_key)
        if value:
            metadatum[key] = value[0]   # All refextract values are lists.
    if 'identifiers' in refextract_metadatum:
        metadatum['identifiers'] = [
            Identifier(**ident) for ident in metadatum['identifiers']
        ]
    if 'author' in refextract_metadatum:
        metadatum['authors'] = [
            Author(fullname=author) for author
            in refextract_metadatum['author']
        ]
    return ExtractedReference(**metadatum)


def extract_references(filename: str, document_id: str) \
        -> List[ExtractedReference]:
    """
    Wrapper for :func:`refextract.extract_references_from_file` function.

    Parameters
    ----------
    filename : str
        Name of the pdf from which to extract references.

    Returns
    -------
    references : list of :class:`ExtractedReference`
        Reference metadata extracted from PDF.
    """
    try:
        return [_transform(reference) for reference
                in refextract.extract_references(filename)]
    except IOError as e:
        raise IOError('Connection to refextract failed: %s' % e) from e
    except Exception as e:
        logger.error('Failed to extract references from %s: %s', filename, e)
        raise RuntimeError('Failed to extract references') from e
