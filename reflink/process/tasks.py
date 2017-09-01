"""
Orchestration module for document processing logic.

This is intended to be the primary entry-point into the processing component of
the service.
"""

from datetime import datetime
from reflink import logging

from reflink.process.retrieve import retrieve
from reflink.process.extract import extract
from reflink.process.merge import merge_records
from reflink.process.store import store, create_success_event, \
                                  create_failed_event
from reflink.services.metrics import metrics

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
@metrics.session.reporter
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
    metrics_data = []
    metadata = []
    start_time = datetime.now()
    logger.info('%s: started processing document' % document_id)
    try:
        # Retrieve PDF from arXiv central object store.
        pdf_path, tex_path = retrieve(document_id)
        if pdf_path is None:
            metrics_data.append({'metric': 'PDFIsAvailable', 'value': 0.})
            msg = '%s: no PDF available' % document_id
            logger.info(msg)
            raise RuntimeError(msg)

        metrics_data.append({'metric': 'PDFIsAvailable', 'value': 1.})
        logger.info('%s: retrieved PDF' % document_id)

        # Extract references using an array of extractors.
        logger.info('%s: extracting metadata' % document_id)
        extractions = extract(pdf_path, document_id)
        metrics_data.append({'metric': 'NumberExtractorsSucceeded',
                             'value': len(extractions)})
        if len(extractions) == 0:
            raise RuntimeError('%s: no extractors succeeded' % document_id)

        logger.info('%s extraction succeeded with %i extractions: %s' %
                    (document_id, len(extractions),
                     ', '.join(extractions.keys())))

        # Merge references across extractors, if more than one succeeded.
        if len(extractions) == 1:
            metadata = extractions.values()[0]
            logger.info('%s: skipping merge step ' % document_id)
        else:
            logger.info('%s: merging metadata ' % document_id)
            metadata, score = merge_records(extractions)
            logger.info('%s: merged, contains %i records with score %f' %
                        (document_id, len(metadata), score))

        # Store final reference set.
        extraction_id = store(metadata, document_id)
        end_time = datetime.now()
        metrics_data.append({'metric': 'FinalQuality', 'value': score})
        metrics_data.append({'metric': 'ProcessingDuration',
                             'value': (start_time - end_time).microseconds,
                             'units': 'Microseconds'})
        create_success_event(extraction_id, document_id, sequence_id)

    except Exception as e:
        logger.error('Failed to process %s: %s' % (document_id, e))
        create_failed_event(document_id, sequence_id)
    finally:
        # MetricsSession.report decorator expects a two-tuple.
        return metadata, metrics_data
