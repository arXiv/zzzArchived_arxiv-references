"""Unit tests for :mod:`references.controllers.extraction`."""

import unittest
from unittest import mock
from references.controllers import extraction
import json


class ExtractionIsRequested(unittest.TestCase):
    """A client POSTs a PDF for extraction."""

    @mock.patch('references.controllers.extraction.referencesStore')
    def setUp(self, mock_ref_store):
        """:class:`.extraction.ExtractionController` handles the request."""
        mock_extractions = mock.MagicMock()
        type(mock_extractions).latest = mock.MagicMock(
            return_value={'document': '1234.5678v2', 'version': 0.1}
        )
        mock_session = mock.MagicMock()
        mock_session.extractions = mock_extractions
        mock_ref_store.session = mock_session
        self.ctrl = extraction.ExtractionController(0.2)

    @mock.patch('references.controllers.extraction.url_for')
    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid(self, mock_process, mock_url_for):
        """The request includes a PDF and required metadata."""
        # process_document is a Celery task.
        mock_result = mock.MagicMock()
        mock_result.task_id = 'qwerty1234'
        mock_delay = mock.MagicMock(return_value=mock_result)
        type(mock_process).delay = mock_delay
        payload = {
            'document_id': '1234.5678v2',
            'url': 'https://arxiv.org/pdf/1234.5678v2'
        }

        def url_for(endpoint, task_id=None):
            """Mock :func:`flask.url_for`."""
            return '/%s/%s' % (endpoint, task_id)
        mock_url_for.side_effect = url_for

        response, status, headers = self.ctrl.extract(payload)

        self.assertEqual(status, 202, "Response status should be 202 Accepted")
        self.assertIn('Location', headers, "Location header should be set")
        self.assertTrue(headers['Location'].endswith('qwerty1234'),
                        "Location header should point to task endpoint.")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    def test_file_is_not_included(self):
        """The request does not include a file."""
        payload = {'document_id': '1234.5678v2'}
        response, status, headers = self.ctrl.extract(payload)
        self.assertEqual(status, 400,
                         "Response status should be 400 Bad Request")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    def test_document_id_is_not_included(self):
        """Document ID is not included in the request payload."""
        payload = {'url': 'https://arxiv.org/pdf/1234.5678v2'}
        response, status, headers = self.ctrl.extract(payload)
        self.assertEqual(status, 400,
                         "Response status should be 400 Bad Request")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")


class ExtractionStatusIsRequested(unittest.TestCase):
    """A client requests the status of their extraction request."""

    @mock.patch('references.controllers.extraction.referencesStore')
    def setUp(self, mock_ref_store):
        """:class:`.extraction.ExtractionController` handles the request."""
        mock_extractions = mock.MagicMock()
        type(mock_extractions).latest = mock.MagicMock(
            return_value={'document': '1234.5678v2', 'version': 0.1}
        )
        mock_session = mock.MagicMock()
        mock_session.extractions = mock_extractions
        mock_ref_store.session = mock_session
        self.ctrl = extraction.ExtractionController(0.2)

    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid_and_task_does_not_exist(self, mock_process):
        """The request includes an id for a task that doesn't exist."""
        mock_async_result = mock.MagicMock()
        type(mock_async_result).status = "PENDING"
        mock_process.async_result = mock_async_result

        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 404,
                         "Response status should be 404 Not Found")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid_and_pending(self, mock_process):
        """The request includes an id for an existing task that is pending."""
        mock_result = mock.MagicMock()
        mock_result.status = "SENT"
        mock_process.async_result = mock.MagicMock(return_value=mock_result)

        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 200, "Response status should be 200 OK")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid_and_started(self, mock_process):
        """The request includes an id for an existing task that is started."""
        mock_result = mock.MagicMock()
        mock_result.status = "STARTED"
        mock_process.async_result = mock.MagicMock(return_value=mock_result)

        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 200, "Response status should be 200 OK")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid_and_failed(self, mock_process):
        """The request includes an id for an existing task that is failed."""
        mock_result = mock.MagicMock()
        mock_result.status = "FAILURE"
        mock_process.async_result = mock.MagicMock(return_value=mock_result)
        mock_result.result = RuntimeError('Something went wrong')
        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 200, "Response status should be 200 OK")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid_and_retrying(self, mock_process):
        """The request includes an id for a task that is being retried."""
        mock_result = mock.MagicMock()
        mock_result.status = "RETRY"
        mock_process.async_result = mock.MagicMock(return_value=mock_result)

        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 200, "Response status should be 200 OK")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    @mock.patch('references.controllers.extraction.url_for')
    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_valid_and_successful(self, mock_process, mock_url_for):
        """The request includes an id for a task that is finished (success)."""
        mock_result = mock.MagicMock()
        mock_result.status = "SUCCESS"

        document_id = '1234.5678v2'
        mock_result.result = {
            "document_id": document_id,
            "references": [],
            "extraction": "bazbat"
        }
        mock_process.async_result = mock.MagicMock(return_value=mock_result)

        def url_for(endpoint, doc_id=None):
            """Mock :func:`flask.url_for`."""
            return '/%s/%s' % (endpoint, doc_id)
        mock_url_for.side_effect = url_for

        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 303,
                         "Response status should be 303 See Other")
        self.assertIn('Location', headers, "Location header should be set")
        self.assertTrue(headers['Location'].endswith(document_id),
                        "Location header should point to ref endpoint.")
        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")

    @mock.patch('references.controllers.extraction.process_document')
    def test_request_is_invalid(self, mock_process):
        """The request includes an id for a task that does not exist."""
        mock_async_result = mock.MagicMock()
        type(mock_async_result).status = "PENDING"
        mock_process.async_result = mock_async_result

        task_id = 'asdf1234-5678'
        response, status, headers = self.ctrl.status(task_id)
        self.assertEqual(status, 404,
                         "Response status should be 404 Not Found")

        try:
            json.dumps(response)
        except TypeError:
            self.fail("Response content should be JSON-serializable")
