import json
import jsonschema
import datetime
import subprocess
import unittest
from unittest import mock

from reflink.process.extract import refextract


class TestRefextractExtractor(unittest.TestCase):
    """Tests for :mod:`reflink.process.extract.refextract` module."""

    def test_extract(self):
        """:func:`.refextract.extract_references` returns valid metadata."""
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

if __name__ == '__main__':
    unittest.main()
