"""Tests :mod:`reflink.process.retrieve` module."""

import unittest
from unittest import mock
import os

from reflink.process.retrieve import retrieve


class TestRetrieve(unittest.TestCase):
    """Test :func:`.retrieve` function."""

    @mock.patch('requests.get')
    def test_retrieve(self, mock_get):
        """:func:`.retrieve` should retrieve PDF and document source files."""
        mock_response = mock.MagicMock()
        mock_response.content = b'foo'
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        try:
            pdf_path, source_path = retrieve('1234.5678')
        except TypeError:
            self.fail('Return value should be a two-tuple.')

        self.assertEqual(mock_get.call_count, 2)
        args, kwargs = mock_get.call_args
        self.assertTrue(args[0].startswith('https://arxiv.org'))
        self.assertTrue(pdf_path.endswith('.pdf'))
        self.assertTrue(source_path.endswith('.tar.gz'))
        self.assertTrue(os.path.exists(pdf_path))
        self.assertTrue(os.path.exists(source_path))

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
        pdf_path, source_path = retrieve('1234.5678')
        self.assertIsInstance(pdf_path, str)
        self.assertEqual(source_path, None)

    @mock.patch('requests.get')
    def test_retrieve_no_pdf(self, mock_get):
        """If no PDF is available, pdf_path should be None."""
        mock_not_found = mock.MagicMock()
        mock_not_found.status_code = 404
        mock_get.return_value = mock_not_found
        pdf_path, source_path = retrieve('1234.5678')
        self.assertEqual(pdf_path, None)
        self.assertEqual(source_path, None)
