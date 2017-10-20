"""Encapsulates storage logic for the processing pipeline."""

from references import logging
from references.config import VERSION
from references.services.events import extractionEvents
from references.services import data_store
# from references.services.data_store import referencesStore
from references.services.metrics import metrics

logger = logging.getLogger(__name__)


@metrics.session.reporter
def create_failed_event(document_id: str, sequence_id: int, *args) -> dict:
    """Commemorate extraction failure."""
    metrics_data = [{'metric': 'ProcessingSucceeded', 'value': 0.}]
    try:
        extractions = extractionEvents.session
        event_data = extractions.update_or_create(sequence_id,
                                                  state=extractions.FAILED,
                                                  document_id=document_id)
    except IOError as e:
        msg = 'Failed to store failed state for %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg)
    return event_data, metrics_data


@metrics.session.reporter
def create_success_event(extraction_id: str, document_id: str,
                         sequence_id: int=-1) -> dict:
    """Commemorate extraction success."""
    metrics_data = [{'metric': 'ProcessingSucceeded', 'value': 1.}]
    if sequence_id == -1:   # Legacy message.
        return
    try:
        extractions = extractionEvents.session
        data = extractions.update_or_create(sequence_id,
                                            state=extractions.COMPLETED,
                                            extraction=extraction_id,
                                            document_id=document_id)
    except IOError as e:
        msg = 'Failed to store success state for %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg)
    return data, metrics_data


def store(metadata: list, document_id: str, score: float=1.0,
          extractors: list=[]) -> str:
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
    logger.info('%s: storing metadata' % document_id)

    try:
        # Returns the data with reference hashes inserted.
        extraction, metadata = data_store.store_references(document_id,
                                                           metadata, VERSION,
                                                           score, extractors)
    except Exception as e:
        msg = '%s: could not store metadata: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    logger.info('%s: stored metadata with extraction %s' %
                (document_id, extraction))
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
    logger.info('%s: storing raw metadata: %s' % (document_id, extractor))

    try:
        data_store.store_raw_extraction(document_id, extractor, metadata)
    except Exception as e:
        msg = '%s: could not store metadata for %s: %s' % \
              (document_id, extractor, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    logger.info('%s: stored metadata for %s' % (document_id, extractor))
