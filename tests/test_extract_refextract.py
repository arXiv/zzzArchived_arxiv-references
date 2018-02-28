"""Tests for :mod:`references.process.extract.refextract` module."""

import json
import jsonschema
import datetime
import os
import subprocess
import unittest
from unittest import mock

from references.domain import ExtractedReference
from references.process.extract import refextract


class TestRefextractExtractor(unittest.TestCase):
    """We'd like to extract references from a PDF using refextract."""

    # @mock.patch('references.services.refextract.requests.get')
    # @mock.patch('references.services.refextract.requests.post')
    @mock.patch('requests.Session')
    def test_extract(self, mock_session):
        """The refextract module generates valid extractions for a PDF."""
        # The extractor service will GET a status endpoint first.

        endpoint_url = 'http://refex/'
        os.environ['REFEXTRACT_ENDPOINT'] = endpoint_url

        mock_get_response = mock.MagicMock()
        mock_get_response.status_code = 200
        mock_get = mock.MagicMock(return_value=mock_get_response)
        mock_session_instance = mock.MagicMock()
        type(mock_session_instance).get = mock_get
        with open('tests/data/refextract.json') as f:
            raw = json.load(f)

        mock_response = mock.MagicMock()
        type(mock_response).json = mock.MagicMock(return_value=raw)
        mock_response.status_code = 200
        mock_response.headers = {'content-type': 'application/json'}
        mock_post = mock.MagicMock(return_value=mock_response)
        type(mock_session_instance).post = mock_post
        mock_session.return_value = mock_session_instance

        pdf_path = 'tests/data/1702.07336.pdf'
        references = refextract.extract_references(pdf_path, '1702.07336')

        self.assertEqual(mock_post.call_count, 1,
                         "The service module should POST.")
        self.assertTrue(mock_post.call_args[0][0].startswith(endpoint_url),
                        "The service should POST to the configured endpoint.")

        self.assertIsInstance(references, list)
        self.assertIsInstance(references[0], ExtractedReference)
        self.assertEqual(len(references), 45)
