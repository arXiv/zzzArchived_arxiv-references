"""Tests for :mod:`references.process.extract.cermine` module."""

import json
import jsonschema
import datetime
import os
import subprocess
import unittest
from unittest import mock

from references.process.extract import cermine


class TestRefextractExtractor(unittest.TestCase):
    """We'd like to extract references from a PDF using CERMINE."""

    @mock.patch('references.services.cermine.requests.post')
    def test_extract(self, mock_post):
        """The cermine module generates valid extractions for a PDF."""
        with open('tests/data/cermine-service-response.xml', 'rb') as f:
            raw = f.read()

        mock_response = mock.MagicMock()
        mock_response.content = raw
        mock_response.status_code = 200
        mock_response.headers = {'content-type': 'application/xml'}
        mock_post.return_value = mock_response
        endpoint_url = 'http://refex/'
        os.environ['CERMINE_ENDPOINT'] = endpoint_url

        pdf_path = 'tests/data/1702.07336.pdf'
        references = cermine.extract_references(pdf_path, '1702.07336')

        self.assertEqual(mock_post.call_count, 1,
                         "The service module should POST.")
        self.assertTrue(mock_post.call_args[0][0].startswith(endpoint_url),
                         "The service should POST to the configured endpoint.")

        self.assertIsInstance(references, list)
        self.assertIsInstance(references[0], dict)
        self.assertEqual(len(references), 1)

        schema_path = 'schema/ExtractedReference.json'
        schemadoc = json.load(open(schema_path))
        try:
            for ref in references:
                jsonschema.validate(ref, schemadoc)
        except jsonschema.exceptions.ValidationError as e:
            self.fail('Invalid reference metadata: %s' % e)
