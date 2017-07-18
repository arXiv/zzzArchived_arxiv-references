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
        pdf_path = 'tests/data/1702.07336.pdf'
        references = refextract.extract_references(pdf_path)
        self.assertIsInstance(references, list)
        self.assertIsInstance(references[0], dict)
        self.assertEqual(len(references), 45)


if __name__ == '__main__':
    unittest.main()
