import json
import jsonschema
import datetime
import subprocess
import unittest
from unittest import mock

from references.process.extract import refextract


class TestRefextractExtractor(unittest.TestCase):
    """Tests for :mod:`references.process.extract.refextract` module."""

    @mock.patch('refextract.extract_references_from_file')
    def test_extract(self, mock_refextract):
        """:func:`.refextract.extract_references` returns valid metadata."""
        with open('tests/data/refextract.json') as f:
            raw = json.load(f)
        # session = mock.MagicMock()
        # type(session).extract_references = mock.MagicMock(return_value=raw)
        mock_refextract.return_value = raw

        pdf_path = 'tests/data/1702.07336.pdf'
        references = refextract.extract_references(pdf_path, '1702.07336')
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
