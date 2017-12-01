"""Encapsulates storage logic for the processing pipeline."""

from references import logging
from references.config import VERSION
from references.services import data_store

logger = logging.getLogger(__name__)


def store(metadata: list, document_id: str, score: float=1.0,
          extractors: list = []) -> str:
    """
    Deposit extracted references in the datastore.

    Parameters
    ----------
    document_id : str
    metadata : str

    Returns
    -------
    str
        Unique identifier for this extraction.
    """
    logger.debug('%s: storing metadata', document_id)
    try:
        # Returns the data with reference hashes inserted.
        extraction, metadata = data_store.store_references(document_id,
                                                           metadata, VERSION,
                                                           score, extractors)
    except Exception as e:
        logger.error('%s: could not store metadata: %s', document_id, e)
        raise RuntimeError('%s: could not store: %s' % (document_id, e)) from e
    logger.debug('%s: refs stored with extraction %s', document_id, extraction)
    return extraction


def store_raw(document_id: str, extractor: str, metadata: list) -> None:
    """
    Deposit raw metadata for a single extractor in the datastore.

    Parameters
    ----------
    document_id : str
        arXiv paper ID or submission ID, preferably including the
        version. E.g. ``"1606.00123v3"``.
    extractor : str
        Name of the reference extractor (e.g. ``"grobid"``).
    references : list
        Extraction metadata. Should be a list of ``dict``, each of
        which represents a single cited reference.

    Raises
    ------
    ValueError
        Invalid value for one or more parameters.
    """
    logger.info('%s: storing raw metadata: %s', document_id, extractor)

    try:
        data_store.store_raw_extraction(document_id, extractor, metadata)
    except Exception as e:
        logger.error('%s: could not store %s: %s', document_id, extractor, e)
        raise RuntimeError('%s: could not store %s: %s' %
                           (document_id, extractor, e)) from e
    logger.debug('%s: refs stored for %s', document_id, extractor)
