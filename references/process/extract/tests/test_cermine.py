"""Tests for :mod:`references.process.extract.cermine` module."""

import json
import jsonschema
import datetime
import os
import subprocess
import unittest
from unittest import mock

from references.process.extract import cermine
from references.domain import Reference


class TestCermineExtractor(unittest.TestCase):
    """CERMINE is available as an HTTP service."""

    @mock.patch('references.services.cermine.requests.Session')
    def test_extract(self, mock_session):
        """The cermine module generates valid extractions for a PDF."""
        # The extractor service will GET a status endpoint first.
        endpoint_url = 'http://cerm.com/'
        os.environ['CERMINE_ENDPOINT'] = endpoint_url
        mock_get_response = mock.MagicMock(status_code=200, ok=True)
        mock_get = mock.MagicMock(return_value=mock_get_response)
        mock_session_instance = mock.MagicMock()
        type(mock_session_instance).get = mock_get

        with open('tests/data/cermine-service-response.xml', 'rb') as f:
            raw = f.read()

        mock_response = mock.MagicMock(content=raw, status_code=200,
                                       headers={'content-type':
                                                'application/xml'})
        mock_post = mock.MagicMock(return_value=mock_response)
        type(mock_session_instance).post = mock_post

        mock_session.return_value = mock_session_instance

        pdf_path = 'tests/data/1702.07336.pdf'
        references = cermine.extract_references(pdf_path, '1702.07336')

        self.assertEqual(mock_post.call_count, 1,
                         "The service module should POST.")
        self.assertTrue(mock_post.call_args[0][0].startswith(endpoint_url),
                        "The service should POST to the configured endpoint.")

        self.assertIsInstance(references, list)
        self.assertIsInstance(references[0], Reference)
        self.assertEqual(len(references), 1)
