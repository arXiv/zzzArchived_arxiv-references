import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

from celery.exceptions import TaskError
from celery import chain

from .retrieve import retrieve
from .extract import extract
from .inject import inject
from .store import store_metadata, store_pdf


def process_document(document_id: str) -> None:
    """
    Orchestrate an asynchronous processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : bytes
    """
    try:
        res = (
            retrieve.s(document_id)
              | extract.s()
              | store_metadata.s(document_id)
              | inject.s()
              | store_pdf.s(document_id)
        ).apply_async()
        logger.info('Started processing document %s' % document_id)
    except TaskError as e:
        msg = 'Could not create processing tasks for %s: %s' % (document_id, e)
        logger.error(msg)
        raise IOError(msg) from e
