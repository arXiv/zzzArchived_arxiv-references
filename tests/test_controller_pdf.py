"""Tests for the :mod:`reflink.web.controllers.pdf` controller."""
import unittest
from unittest import mock
from reflink.web.controllers import pdf
from moto import mock_s3
from reflink.services import object_store
from reflink import status


class TestPDFController(unittest.TestCase):
    """Test the :class:`.pdf.PDFController` controller."""

    @mock_s3
    @mock.patch.object(object_store.PDFStoreSession, 'retrieve_url')
    def test_get_calls_datastore_session(self, retrieve_url_mock):
        """Test the :meth:`.pdf.PDFController.get` method with a valid ID."""
        retrieve_url_mock.return_value = []
        controller = pdf.PDFController()

        try:
            response, status_code = controller.get('arxiv:1234.5678')
        except TypeError:
            self.fail('PDFController.get should return a tuple')
        self.assertEqual(retrieve_url_mock.call_count, 1)

    @mock_s3
    @mock.patch.object(object_store.PDFStoreSession, 'retrieve_url')
    def test_get_handles_IOError(self, retrieve_url_mock):
        """Test the case that the underlying object store raises an IOError."""
        def raise_ioerror(*args):
            raise IOError('Whoops!')
        retrieve_url_mock.side_effect = raise_ioerror

        controller = pdf.PDFController()

        try:
            response, status_code = controller.get('arxiv:1234.5678')
        except TypeError:
            self.fail('PDFController.get should return a tuple')

        self.assertEqual(status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    @mock_s3
    @mock.patch.object(object_store.PDFStoreSession, 'retrieve_url')
    def test_get_handles_nonexistant_record(self, retrieve_url_mock):
        """Test the case that a non-existant object is requested."""
        retrieve_url_mock.return_value = None

        controller = pdf.PDFController()

        try:
            response, status_code = controller.get('arxiv:1234.5678')
        except TypeError:
            self.fail('PDFController.get should return a tuple')

        self.assertEqual(status_code, status.HTTP_404_NOT_FOUND)


if __name__ == '__main__':
    unittest.main()
