"""Tests for :mod:`references.process.extract.refextract` module."""

import json
import jsonschema
import datetime
import os
import subprocess
import unittest
from unittest import mock

from references.process.extract import refextract


class TestRefextractExtractor(unittest.TestCase):
    """We'd like to extract references from a PDF using refextract."""

    @mock.patch('references.services.refextract.requests.post')
    def test_extract(self, mock_post):
        """The refextract module generates valid extractions for a PDF."""
        with open('tests/data/refextract.json') as f:
            raw = json.load(f)

        mock_response = mock.MagicMock()
        type(mock_response).json = mock.MagicMock(return_value=raw)
        mock_response.status_code = 200
        mock_response.headers = {'content-type': 'application/json'}
        mock_post.return_value = mock_response
        endpoint_url = 'http://refex/foo/'
        os.environ['REFEXTRACT_ENDPOINT'] = endpoint_url

        pdf_path = 'tests/data/1702.07336.pdf'
        references = refextract.extract_references(pdf_path, '1702.07336')

        self.assertEqual(mock_post.call_count, 1,
                         "The service module should POST.")
        self.assertTrue(mock_post.call_args[0][0].startswith(endpoint_url),
                        "The service should POST to the configured endpoint.")

        self.assertIsInstance(references, list)
        self.assertIsInstance(references[0], dict)
        self.assertEqual(len(references), 45)

        schema_path = 'schema/ExtractedReference.json'
        schemadoc = json.load(open(schema_path))
        try:
            for ref in references:
                jsonschema.validate(ref, schemadoc)
        except jsonschema.exceptions.ValidationError as e:
            self.fail('Invalid reference metadata: %s' % e)
