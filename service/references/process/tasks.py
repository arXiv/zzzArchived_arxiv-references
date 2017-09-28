"""

"""

from datetime import datetime
from references import logging

from references.process.extract import extract
from references.process.merge import merge_records
from references.process.store import store
from references.services.metrics import metrics

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
@metrics.session.reporter
def process_document(document_id: str, pdf_path: str) -> list:
    """
    Processing chain for a single arXiv document.

    Parameters
    ----------
    document_id : pdf_path
    pdf_path : str

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

    except Exception as e:
        logger.error('Failed to process %s: %s' % (document_id, e))
    finally:
        # MetricsSession.report decorator expects a 2-tuple.
        return {
            'references': metadata,
            'extraction': extraction_id
        }, metrics_data
