"""Asynchronous tasks for reference extraction."""
import os
from datetime import datetime
from typing import List, Any

from references.domain import ReferenceSet, Reference
from references.process.extract import extract
from references.process.merge import merge_records
from references.services import retrieve, data_store
from arxiv.base import logging
from arxiv.base.globals import get_application_config

from celery import shared_task
from celery.result import AsyncResult
from celery import current_app
from celery.signals import after_task_publish

logger = logging.getLogger(__name__)


def _fail(document_id: str, e: Exception, reason: str) -> None:
    """Helper to log exceptions consistently before propagating."""
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
    config = get_application_config()
    logger.debug('%s: started processing document',  document_id)

    # Retrieve PDF from arXiv central document store.
    try:
        pdf_path = retrieve.retrieve_pdf(pdf_url, document_id)
    except retrieve.PDFNotFound:
        _fail(document_id, RuntimeError('PDF not found'), 'PDF not found')
    except retrieve.RetrieveFailed as e:
        _fail(document_id, e, "failed to retrieve PDF")
    except retrieve.InvalidURL as e:
        _fail(document_id, e, "failed to retrieve PDF")

    logger.info('%s: retrieved PDF', document_id)

    # Extract references using an array of extractors.
    logger.debug('%s: extracting metadata', document_id)
    extractions = extract(pdf_path, document_id)

    if len(extractions) == 0:
        _fail(document_id, RuntimeError("no extractors succeeded"),
              "no extractors succeeded")

    logger.debug('%s extraction succeeded with %i extractions: %s',
                 document_id, len(extractions), ', '.join(extractions.keys()))

    # Attempt to store raw extraction metadata for each extractor.
    now = datetime.now()
    for extractor_name, extractor_metadata in extractions.items():
        reference_set = ReferenceSet(      # type: ignore
            document_id=document_id,
            references=extractor_metadata,
            version=config['VERSION'],
            score=0.0,
            created=now,
            updated=now,
            extractor=extractor_name,
            raw=True
        )
        try:
            data_store.save(reference_set)
        except IOError as e:
            logger.error('%s: could not store raw: %s', document_id, e)

    # Merge references across extractors, if more than one succeeded.
    metadata: List[Reference]
    try:
        logger.debug('%s: merging metadata', document_id)
        metadata, score = merge_records(extractions)
        logger.debug('%s: merged, contains %i records with score %f',
                     document_id, len(metadata), score)
    except Exception as e:
        _fail(document_id, e, "merge failed")

    # Store final reference set.
    try:
        reference_set = ReferenceSet(   # type: ignore
            document_id=document_id,
            references=metadata,
            version=config['VERSION'],
            score=score,
            created=now,
            updated=now,
            extractors=list(extractions.keys())
        )
        data_store.save(reference_set)
    except Exception as e:
        _fail(document_id, e, "store failed")

    logger.info('%s: finished extracting metadata', document_id)
    if os.path.exists(pdf_path):
        os.remove(pdf_path)
    return {
        'document_id': document_id,
        'references': metadata
    }


# We want to be able to use this without introducing Celery explicitly in
#  upstream modules.
process_document.async_result = AsyncResult


@after_task_publish.connect
def update_sent_state(sender: Any = None, headers: dict = {},
                      body: Any = None, **kwargs: Any) -> None:
    """Set state to SENT, so that we can tell whether a task exists."""
    task = current_app.tasks.get(sender)
    backend = task.backend if task else current_app.backend
    backend.store_result(headers['id'], None, "SENT")
