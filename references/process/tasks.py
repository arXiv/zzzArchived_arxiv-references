"""

"""

from datetime import datetime
from references import logging

from references.process.extract import extract
from references.process.merge import merge_records
from references.process.store import store, store_raw
from references.process.retrieve import retrieve
from references.services import metrics

from celery import shared_task
from celery.result import AsyncResult
from celery import current_app
from celery.signals import after_task_publish

logger = logging.getLogger(__name__)


@shared_task
def process_document(document_id: str, pdf_url: str) -> list:
    """
    Processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : pdf_path
    pdf_url : str

    Returns
    -------
    list
        Extracted reference metadata.

    Raises
    ------
    RuntimeError
    """
    # These are set up here so that they are always defined in the finally
    # block, below.
    metadata = []
    extraction_id = None

    start_time = datetime.now()
    logger.info('%s: started processing document' % document_id)
    try:
        # Retrieve PDF from arXiv central document store.
        pdf_path = retrieve(pdf_url, document_id)
        if pdf_path is None:
            metrics.report('PDFIsAvailable', 0.)
            msg = '%s: no PDF available' % document_id
            logger.info(msg)
            raise RuntimeError(msg)
        metrics.report('PDFIsAvailable', 1.)
        logger.info('%s: retrieved PDF' % document_id)

        # Extract references using an array of extractors.
        logger.info('%s: extracting metadata' % document_id)
        extractions = extract(pdf_path, document_id)
        metrics.report('NumberExtractorsSucceeded', len(extractions))

        if len(extractions) == 0:
            raise RuntimeError('%s: no extractors succeeded' % document_id)

        logger.info('%s extraction succeeded with %i extractions: %s' %
                    (document_id, len(extractions),
                     ', '.join(extractions.keys())))

        for extractor_name, extractor_metadata in extractions.items():
            try:
                store_raw(document_id, extractor_name, extractor_metadata)
            except IOError as e:
                logger.error('%s: Could not store raw extraction: %s' %
                             (document_id, e))

        # Merge references across extractors, if more than one succeeded.
        # if len(extractions) == 1:
        #     metadata = list(extractions.values())[0]
        #     logger.info('%s: skipping merge step ' % document_id)
        # else:
        logger.info('%s: merging metadata ' % document_id)
        metadata, score = merge_records(extractions)
        logger.info('%s: merged, contains %i records with score %f' %
                    (document_id, len(metadata), score))

        # Store final reference set.
        extraction_id = store(metadata, document_id, score=score,
                              extractors=list(extractions.keys()))
        end_time = datetime.now()
        metrics.report('FinalQuality', score)
        metrics.report('ProcessingDuration',
                       (start_time - end_time).microseconds, 'Microseconds')
    except Exception as e:
        logger.error('Failed to process %s: %s' % (document_id, e))
        raise e
    return {
        'document_id': document_id,
        'references': metadata,
        'extraction': extraction_id
    }


# We want to be able to use this without introducing Celery explicitly in
#  upstream modules.
process_document.async_result = AsyncResult


@after_task_publish.connect
def update_sent_state(sender=None, headers=None, body=None, **kwargs):
    """Set state to SENT, so that we can tell whether a task exists."""
    task = current_app.tasks.get(sender)
    backend = task.backend if task else current_app.backend
    backend.store_result(headers['id'], None, "SENT")
