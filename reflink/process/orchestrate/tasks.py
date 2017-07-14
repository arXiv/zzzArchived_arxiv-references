"""
Orchestration module for document processing logic.

This is intended to be the primary entry-point into the processing component of
the service.
"""

import logging

from reflink.config import VERSION
from reflink.process.retrieve import retrieve
from reflink.process.extract import extract
from reflink.process.inject import inject
from reflink.process.store import store_metadata, store_pdf

from celery import shared_task

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
logger = logging.getLogger(__name__)


@shared_task
def process_document(document_id: str) -> None:
    """
    Orchestrate an asynchronous processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : bytes
    """
    logger.info('Started processing document %s' % document_id)
    try:
        pdf_path, tex_path = retrieve(document_id)
        logger.info('Retrieved content for %s' % document_id)

        logger.info('Extracting metadata for %s' % document_id)
        metadata = extract(pdf_path)    # TODO: add reconciliation step.
        logger.info('Extracted metadata for %s' % document_id)
        # logger.debug('Extracted: %s' % metadata)

        # Should return the data with reference hashes inserted.
        logger.info('Storing metadata for %s' % document_id)
        extraction, metadata = store_metadata(metadata, document_id, VERSION)
        logger.info('Stored metadata for %s' % document_id)
        # logger.debug('Stored: %s' % str(metadata))

        logger.info('Injecting links for %s' % document_id)
        logger.debug('Injecting in source: %s' % tex_path)
        new_pdf_path = inject(tex_path, metadata)
        logger.info('Created injected PDF for %s' % document_id)
        logger.debug('PDF at %s' % new_pdf_path)

        logger.info('Storing injected PDF for %s' % document_id)
        store_pdf(new_pdf_path, document_id)
        logger.info('Stored injected PDF for %s' % document_id)
    except Exception as e:
        msg = 'Failed to process %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
