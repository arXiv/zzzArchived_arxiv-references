"""Encapsulates storage logic for the processing pipeline."""

from reflink.services import object_store
from reflink import logging
from reflink.config import VERSION
from reflink.services.events import extractionEvents
from reflink.services.data_store import referencesStore
from reflink.services.metrics import metrics

logger = logging.getLogger(__name__)


def store_pdf(pdf_path: str, document_id: str) -> None:
    """
    Deposit link-injected PDF in the object store.

    Parameters
    ----------
    pdf_path : str
        The location of the link-injected PDF on the filesystem.
    document_id : str

    Raises
    ------
    RuntimeError
        Raised when there is a problem storing the PDF. The caller should
        assumethat nothing has been stored.
    """
    try:
        object_store.get_session().create(document_id, pdf_path)
    except IOError as e:    # Separating this out in case we want to retry.
        msg = 'Could not store PDF for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    except Exception as e:
        msg = 'Could not store PDF for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e


def create_failed_event(document_id: str, sequence_id: int, *args) -> dict:
    """Commemorate extraction failure."""

    metrics.session.report('ProcessingSucceeded', 0.)
    try:
        extractions = extractionEvents.session
        event_data = extractions.update_or_create(sequence_id,
                                                  state=extractions.FAILED,
                                                  document_id=document_id)
    except IOError as e:
        msg = 'Failed to store failed state for %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg)
    return event_data


def create_success_event(extraction_id: str, document_id: str,
                         sequence_id: int=-1) -> dict:
    """Commemorate extraction success."""

    metrics.session.report('ProcessingSucceeded', 1.)
    if sequence_id == -1:   # Legacy message.
        return
    try:
        extractions = extractionEvents.session
        data = extractions.create(sequence_id, state=extractions.COMPLETED,
                                  extraction=extraction_id,
                                  document_id=document_id)
    except IOError as e:
        msg = 'Failed to store success state for %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg)
    return data


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
    logger.info('Storing metadata for %s' % document_id)

    try:
        # Should return the data with reference hashes inserted.
        extraction, metadata = referencesStore.session.create(document_id,
                                                              metadata,
                                                              VERSION)
    except IOError as e:    # Separating this out in case we want to retry.
        msg = 'Could not store metadata for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    except Exception as e:
        msg = 'Could not store metadata for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    logger.info('Stored metadata for %s with extraction %s' %
                (document_id, extraction))
    return extraction
