"""
Orchestration module for document processing logic.

This is intended to be the primary entry-point into the processing component of
the service.
"""

from reflink import logging

from reflink.config import VERSION
from reflink.process.retrieve import retrieve
from reflink.process.extract import extract
# from reflink.process.inject import inject
from reflink.process.merge import merge

from celery import shared_task

logger = logging.getLogger(__name__)





@shared_task
def process_document(document_id: str) -> list:
    """
    Processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : bytes

    Returns
    -------
    list
        Extracted reference metadata.

    Raises
    ------
    RuntimeError
    """
    logger.info('Started processing document %s' % document_id)
    try:
        pdf_path, tex_path = retrieve(document_id)
        logger.info('Retrieved content for %s' % document_id)
        if pdf_path is None:
            logger.info('No PDF available for %s; aborting' % document_id)
            return

        logger.info('Extracting metadata for %s' % document_id)
        extractions = extract(pdf_path, document_id)
        logger.info('Extraction for %s succeeded with %i extractions: %s' %
                    (document_id, len(extractions),
                     ', '.join(extractions.keys())))

        if len(extractions) > 1:
            logger.info('Merging metadata for %s' % document_id)
            metadata, score = merge.merge_records(extractions)
            logger.info('Merged: %s contains %i records with score %f' %
                        (document_id, len(metadata), score))
        else:
            metadata = extractions
            logger.info('Skipping merge step for %s' % document_id)

        # 2017-07-31: disabling link injection for now. - Erick
        #
        # if tex_path:
        #     logger.info('Injecting links for %s' % document_id)
        #     logger.debug('Injecting in source: %s' % tex_path)
        #     new_pdf_path = inject(tex_path, metadata)
        #     logger.info('Created injected PDF for %s' % document_id)
        #     logger.debug('PDF at %s' % new_pdf_path)
        #
        #     logger.info('Storing injected PDF for %s' % document_id)
        #     store_pdf(new_pdf_path, document_id)
        #     logger.info('Stored injected PDF for %s' % document_id)
    except Exception as e:
        msg = 'Failed to process %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e

    return metadata
