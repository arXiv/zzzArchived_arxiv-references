"""Controller for extraction requests."""

from references import status, logging
from references.services.data_store import referencesStore
from references.process.tasks import process_document
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
        self.extractions = referencesStore.session.extractions

    def extract(self, payload: str) -> tuple:
        """Handle a request for reference extraction."""
        document_id = payload.get('document_id')
        if document_id is None or not isinstance(document_id, str):
            return DOCUMENT_ID_MISSING, status.HTTP_400_BAD_REQUEST, {}

        latest = self.extractions.latest(document_id)
        if latest is not None and latest.get('version') >= self.current:
            headers = {'Location': url_for('references', doc_id=document_id)}
            return ALREADY_EXISTS, status.HTTP_303_SEE_OTHER, headers

        pdf_url = payload.get('url')
        if pdf_url is None or not is_valid_url(pdf_url):
            return FILE_MISSING_OR_INVALID, status.HTTP_400_BAD_REQUEST, {}

        result = process_document.delay(document_id, pdf_url)
        headers = {'Location': url_for('task_status', task_id=result.task_id)}
        return ACCEPTED, status.HTTP_202_ACCEPTED, headers

    def status(self, task_id: str) -> tuple:
        """Check the status of an extraction request."""
        if not isinstance(task_id, str):
            raise ValueError('task_id must be string, not %s' % type(task_id))
        result = process_document.async_result(task_id)
        if result.status == 'PENDING':
            return TASK_DOES_NOT_EXIST, status.HTTP_404_NOT_FOUND, {}
        elif result.status in ['SENT', 'STARTED', 'RETRY']:
            return TASK_IN_PROGRESS, status.HTTP_200_OK, {}
        elif result.status == 'FAILURE':
            logger.error('%s: failed task: %s' % (task_id, result.result))
            return TASK_FAILED, status.HTTP_200_OK, {}
        elif result.status == 'SUCCESS':
            document_id = result.result.get('document_id')
            headers = {'Location': url_for('references', doc_id=document_id)}
            return TASK_COMPLETE, status.HTTP_303_SEE_OTHER, headers
        return TASK_DOES_NOT_EXIST, status.HTTP_404_NOT_FOUND, {}
