"""
Orchestration module for document processing logic.

This is intended to be the primary entry-point into the processing component of
the service.
"""

import logging

from celery.exceptions import TaskError
from celery import group

from reflink.process.retrieve.tasks import retrieve
from reflink.process.extract.tasks import extract
from reflink.process.inject.tasks import inject
from reflink.process.store.tasks import store_metadata, store_pdf

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def process_document(document_id: str) -> None:
    """
    Orchestrate an asynchronous processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : bytes
    """
    try:
        (retrieve.s(document_id)
         | extract.s()
         | inject.s()
         | group(store_metadata.s(document_id), store_pdf.s(document_id))
         ).apply_async()
        logger.info('Started processing document %s' % document_id)
    except TaskError as e:
        msg = 'Could not create processing tasks for %s: %s' % (document_id, e)
        logger.error(msg)
        raise IOError(msg) from e
