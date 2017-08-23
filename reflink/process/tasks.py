"""
Orchestration module for document processing logic.

This is intended to be the primary entry-point into the processing component of
the service.
"""

from datetime import datetime
from reflink import logging

from reflink.process.retrieve import retrieve
from reflink.process.extract import extract
# from reflink.process.inject import inject
from reflink.process.merge import merge
from reflink.process.store import store, create_success_event, \
                                  create_failed_event
from reflink.services import Metrics

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
def process_document(document_id: str, sequence_id: int) -> list:
    """
    Processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : bytes
    sequence_id : int

    Returns
    -------
    list
        Extracted reference metadata.

    Raises
    ------
    RuntimeError
    """
    metrics_session = Metrics().session
    start_time = datetime.now()
    logger.info('Started processing document %s' % document_id)
    try:
        pdf_path, tex_path = retrieve(document_id)
        if pdf_path is None:
            logger.info('No PDF available for %s; aborting' % document_id)
            metrics_session.report('PDFIsAvailable', 0.)
            return []
        metrics_session.report('PDFIsAvailable', 1.)
        logger.info('Retrieved content for %s' % document_id)

        logger.info('Extracting metadata for %s' % document_id)
        extractions = extract(pdf_path, document_id)

        metrics_session.report('NumberExtractorsSucceeded', len(extractions))
        if len(extractions) == 0:
            raise RuntimeError('No extractors succeeded for %s' % document_id)
        else:
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

        extraction_id = store(metadata, document_id)
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
        create_failed_event(document_id, sequence_id)
        return []
    end_time = datetime.now()

    metrics_session.report('FinalQuality', score)
    metrics_session.report('ProcessingDuration',
                           (start_time - end_time).microseconds,
                           units='Microseconds')
    create_success_event(extraction_id, document_id, sequence_id)
    return metadata
