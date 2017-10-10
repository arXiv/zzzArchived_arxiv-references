"""Controller for extraction requests."""

from references import status, logging
from references.services.data_store import referencesStore
from references.process.tasks import process_document, AsyncResult
from references.process.retrieve import is_valid_url

from flask import url_for

logger = logging.getLogger(__name__)

DOCUMENT_ID_MISSING = {'reason': 'document_id missing in request'}
FILE_MISSING_OR_INVALID = {'reason': 'file not found or invalid'}
ACCEPTED = {'reason': 'reference extraction in process'}
ALREADY_EXISTS = {'reason': 'extraction already exists'}
TASK_DOES_NOT_EXIST = {'reason': 'task not found'}
TASK_IN_PROGRESS = {'status': 'in progress'}
TASK_FAILED = {'status': 'failed'}
TASK_COMPLETE = {'status': 'complete'}


class ExtractionController(object):
    """Responsible for requests for reference extraction."""

    def __init__(self, current_version: float=0.0):
        """Get a session with the reference store."""
        self.current = current_version
        logger.info('ExtractionController.__init__')
        self.extractions = referencesStore.session.extractions
        logger.info('Ready')

    def extract(self, payload: str) -> tuple:
        """Handle a request for reference extraction."""
        document_id = payload.get('document_id')
        if document_id is None or not isinstance(document_id, str):
            return DOCUMENT_ID_MISSING, status.HTTP_400_BAD_REQUEST, {}
        logger.info('extract: got document_id: %s' % document_id)

        latest = self.extractions.latest(document_id)
        if latest is not None and latest.get('version') >= self.current:
            headers = {
                'Location': url_for('references.references',
                                    doc_id=document_id)
            }
            return ALREADY_EXISTS, status.HTTP_303_SEE_OTHER, headers
        logger.info('extract: got latest: %s' % latest)

        pdf_url = payload.get('url')
        if pdf_url is None or not is_valid_url(pdf_url):
            return FILE_MISSING_OR_INVALID, status.HTTP_400_BAD_REQUEST, {}
        logger.info('extract: got url: %s' % pdf_url)

        result = process_document.delay(document_id, pdf_url)
        logger.info('extract: started processing as %s' % result.task_id)
        headers = {'Location': url_for('references.task_status',
                                       task_id=result.task_id)}
        return ACCEPTED, status.HTTP_202_ACCEPTED, headers

    def status(self, task_id: str) -> tuple:
        """Check the status of an extraction request."""
        logger.debug('%s: Get status for task' % task_id)
        if not isinstance(task_id, str):
            logger.debug('%s: Failed, invalid task id' % task_id)
            raise ValueError('task_id must be string, not %s' % type(task_id))
        result = AsyncResult(task_id)
        logger.debug('%s: got result: %s' % (task_id, result.status))
        if result.status == 'PENDING':
            return TASK_DOES_NOT_EXIST, status.HTTP_404_NOT_FOUND, {}
        elif result.status in ['SENT', 'STARTED', 'RETRY']:
            return TASK_IN_PROGRESS, status.HTTP_200_OK, {}
        elif result.status == 'FAILURE':
            logger.error('%s: failed task: %s' % (task_id, result.result))
            reason = TASK_FAILED
            reason.update({'reason': str(result.result)})
            return reason, status.HTTP_200_OK, {}
        elif result.status == 'SUCCESS':
            task_result = result.result
            logger.debug(str(task_result))
            document_id = result.result.get('document_id')
            headers = {'Location': url_for('references.references',
                                           doc_id=document_id)}
            return TASK_COMPLETE, status.HTTP_303_SEE_OTHER, headers
        return TASK_DOES_NOT_EXIST, status.HTTP_404_NOT_FOUND, {}
