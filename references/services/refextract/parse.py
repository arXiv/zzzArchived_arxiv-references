"""
Provides extractions using the refextract package.

See <https://github.com/inspirehep/refextract>.
"""

from typing import List, Any, Dict

from references.domain import Reference, Identifier, Author
from arxiv.base import logging
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


def transform(refextract_metadatum: dict) -> Reference:
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
            Identifier(**ident)     # type: ignore
            for ident in metadatum['identifiers']
        ]
    if 'author' in refextract_metadatum:
        metadatum['authors'] = [
            Author(fullname=author)     # type: ignore
            for author in refextract_metadatum['author']
        ]
    return Reference(**metadatum)   # type: ignore
