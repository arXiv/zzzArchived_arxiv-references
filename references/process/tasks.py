"""

"""
import os
from datetime import datetime
from typing import List

from references.process.extract import extract
from references.process.merge import merge_records
from references.process.store import store, store_raw
from references.services import metrics, retrieve
from references import logging

from celery import shared_task
from celery.result import AsyncResult
from celery import current_app
from celery.signals import after_task_publish

logger = logging.getLogger(__name__)


def _fail(document_id: str, e: Exception, reason: str) -> None:
    """"""
    metrics.report('ProcessingSucceeded', 0.)
    logger.error('%s: failed to process: %s', document_id, reason)
    raise e


@shared_task
def process_document(document_id: str, pdf_url: str) -> dict:
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
    metadata: List[dict] = []
    extraction_id = None

    start_time = datetime.now()
    logger.debug('%s: started processing document',  document_id)

    # Retrieve PDF from arXiv central document store.
    try:
        pdf_path = retrieve.retrieve_pdf(pdf_url, document_id)
        if pdf_path is None:
            metrics.report('PDFIsAvailable', 0.)
            _fail(document_id, RuntimeError('No PDF available'),
                  'no PDF available')
    except IOError as e:
        metrics.report('PDFIsAvailable', 0.)
        _fail(document_id, e, "failed to retrieve PDF")
    except ValueError as e:
        _fail(document_id, e, "failed to retrieve PDF")

    metrics.report('PDFIsAvailable', 1.)
    logger.info('%s: retrieved PDF', document_id)

    # Extract references using an array of extractors.
    logger.debug('%s: extracting metadata', document_id)
    extractions = extract(pdf_path, document_id)
    metrics.report('NumberExtractorsSucceeded', len(extractions))

    if len(extractions) == 0:
        _fail(document_id, RuntimeError("no extractors succeeded"),
              "no extractors succeeded")

    logger.debug('%s extraction succeeded with %i extractions: %s',
                 document_id, len(extractions), ', '.join(extractions.keys()))

    # Attempt to store raw extraction metadata for each extractor.
    for extractor_name, extractor_metadata in extractions.items():
        try:
            store_raw(document_id, extractor_name, extractor_metadata)
        except IOError as e:
            logger.error('%s: could not store raw: %s', document_id, e)

    # Merge references across extractors, if more than one succeeded.
    try:
        logger.debug('%s: merging metadata', document_id)
        metadata, score = merge_records(extractions)
        logger.debug('%s: merged, contains %i records with score %f',
                     document_id, len(metadata), score)
    except Exception as e:
        _fail(document_id, e, "merge failed")

    # Store final reference set.
    try:
        extraction_id = store(metadata, document_id, score=score,
                              extractors=list(extractions.keys()))
    except Exception as e:
        _fail(document_id, e, "store failed")

    end_time = datetime.now()
    metrics.report('FinalQuality', score)
    metrics.report('ProcessingDuration',
                   (start_time - end_time).microseconds, 'Microseconds')
    metrics.report('ProcessingSucceeded', 1.)
    logger.info('%s: finished extracting metadata', document_id)
    if os.path.exists(pdf_path):
        os.remove(pdf_path)
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
