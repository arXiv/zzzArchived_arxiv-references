"""Tests :mod:`references.process.retrieve` module."""

import unittest
from unittest import mock
from moto import mock_cloudwatch
import os
from references.process import retrieve


class TestRetrieve(unittest.TestCase):
    """Test :func:`.retrieve` function."""

    @mock_cloudwatch
    @mock.patch('requests.get')
    def test_retrieve(self, mock_get):
        """:func:`.retrieve` should retrieve a PDF."""
        mock_response = mock.MagicMock()
        mock_response.content = b'foo'
        mock_response.status_code = 200
        mock_response.headers = {'content-type': 'application/pdf'}
        mock_get.return_value = mock_response

        pdf_path = retrieve.retrieve('https://arxiv.org/pdf/1234.5678',
                                     '1234.5678')

        self.assertEqual(mock_get.call_count, 1)
        args, kwargs = mock_get.call_args
        self.assertTrue(pdf_path.endswith('.pdf'))
        self.assertTrue(os.path.exists(pdf_path))

    @mock_cloudwatch
    @mock.patch('requests.get')
    def test_retrieve_no_source(self, mock_get):
        """If no source is available, the PDF should still be retrieved."""
        mock_pdf_response = mock.MagicMock()
        mock_pdf_response.content = b'foo'
        mock_pdf_response.status_code = 200

        mock_source_response = mock.MagicMock()
        mock_source_response.status_code = 404

        def _handle_request(url):
            if 'pdf' in url:
                return mock_pdf_response
            elif 'e-print' in url:
                return mock_source_response

        mock_get.side_effect = _handle_request
        pdf_path = retrieve.retrieve('https://arxiv.org/pdf/1234.5678',
                                     '1234.5678')
        self.assertIsInstance(pdf_path, str)

    @mock_cloudwatch
    @mock.patch('requests.get')
    def test_retrieve_no_pdf(self, mock_get):
        """If no PDF is available, pdf_path should be None."""
        mock_not_found = mock.MagicMock()
        mock_not_found.status_code = 404
        mock_get.return_value = mock_not_found
        pdf_path = retrieve.retrieve('https://arxiv.org/pdf/1234.5678',
                                     '1234.5678')
        self.assertEqual(pdf_path, None)
