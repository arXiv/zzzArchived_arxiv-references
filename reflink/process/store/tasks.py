"""Implements data storage tasks for the processing pipeline."""

import logging

from reflink.services import data_store, object_store
from celery import shared_task

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
logger = logging.getLogger(__name__)


@shared_task
def store_metadata(extraction_injection_results: tuple,
                   document_id: str) -> None:
    """
    Deposit extracted reference metadata in the datastore.

    Parameters
    ----------
    extraction_injection_results : tuple
        The final results from the extraction and injection process, comprised
        of metadata (list) and the path to the link-injected PDF (str).
    document_id : str

    Raises
    ------
    RuntimeError
        Raised when there is a problem storing the metadata. The caller should
        assume that nothing has been stored.
    """
    metadata, _ = extraction_injection_results
    try:
        data_store.get_session().create(document_id, metadata)
    except IOError as e:    # Separating this out in case we want to retry.
        msg = 'Could not store metadata for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    except Exception as e:
        msg = 'Could not store metadata for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    return metadata


@shared_task
def store_pdf(extraction_injection_results: tuple, document_id: str) -> None:
    """
    Deposit link-injected PDF in the object store.

    Parameters
    ----------
    extraction_injection_results : tuple
        The final results from the extraction and injection process, comprised
        of metadata (list) and the path to the link-injected PDF (str).
    document_id : str

    Raises
    ------
    RuntimeError
        Raised when there is a problem storing the PDF. The caller should
        assumethat nothing has been stored.
    """
    _, pdf_path = extraction_injection_results
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
