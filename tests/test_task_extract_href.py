import unittest
import json
import jsonschema

from reflink.process.extract import href

import logging
logging.getLogger().setLevel(logging.ERROR)


class TestHREFExtraction(unittest.TestCase):
    """Tests for :mod:`reflink.process.extract.href` module."""

    def test_extract_returns_valid_metadata(self):
        """The :func:`.href.extract_references` returns valid metadata."""

        pdf_path = 'tests/data/1702.07336.pdf'
        references = href.extract_references(pdf_path)
        self.assertIsInstance(references, list)
        self.assertIsInstance(references[0], dict)
        self.assertEqual(len(references), 5)

        schema_path = 'schema/ExtractedReference.json'
        schemadoc = json.load(open(schema_path))
        try:
            for ref in references:
                jsonschema.validate(ref, schemadoc)
                self.assertIsInstance(ref, dict,
                                      "Reference should be a native dict")
                self.assertTrue('href' in ref)
                self.assertTrue(ref['href'].startswith('http://'))
        except jsonschema.exceptions.ValidationError as e:
            self.fail('Invalid reference metadata: %s' % e)
