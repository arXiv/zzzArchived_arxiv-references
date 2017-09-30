"""Encapsulates storage logic for the processing pipeline."""

from references import logging
from references.config import VERSION
from references.services.events import extractionEvents
from references.services.data_store import referencesStore
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


def store(metadata: list, document_id: str) -> str:
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
        extraction, metadata = referencesStore.session.create(document_id,
                                                              metadata,
                                                              VERSION)
    except Exception as e:
        msg = '%s: could not store metadata: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    logger.info('%s: stored metadata with extraction %s' %
                (document_id, extraction))
    return extraction
