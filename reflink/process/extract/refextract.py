"""
Provides extractions using the `refextract
<https://github.com/inspirehep/refextract>`_ package.
"""


from refextract import extract_references_from_file
import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    level=logging.DEBUG
)

logging.getLogger('refextract.references.engine').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    metadatum = {}
    for re_key, key in FIELD_MAPPINGS:
        value = refextract_metadatum.get(re_key)
        if value:
            metadatum[key] = value[0]   # All refextract values are lists.
    if 'author' in refextract_metadatum:
        metadatum['authors'] = [{'fullname': author} for author
                                in refextract_metadatum['author']]
    return metadatum


def extract_references(filename: str) -> str:
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
    try:
        return [_transform(reference) for reference
                in extract_references_from_file(filename)]
    except IOError as e:
        raise IOError('%s' % e) from e
    except Exception as e:
        msg = 'Failed to extract references from %s' % filename
        raise RuntimeError(msg) from e
