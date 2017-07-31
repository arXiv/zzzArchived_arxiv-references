import json
import unittest

from reflink.process.merge import validate


class TestValidateRecords(unittest.TestCase):
    """Tests for :func:`reflink.process.merge.validate.validate` function."""

    def test_simple_records(self):
        """Validate some aligned records."""
        aligned_records = [
            [
                ["ext1", {"title": "Matt", "cheatcode": "uuddlrlrba", "year": 2011}],
                ["ext2", {"title": "Matt", "cheatcode": "uuddlrlrbaba","year": 2011}]
            ],
            [
                ["ext1", {"title": "Erick", "cheatcode": "babaudbalrba", "year": 2013}],
                ["ext3", {'title': 'Eric', 'cheatcode': 'babaudbalrba', 'year': 2013}]
            ],
            [
                ["ext3", {"title": "John", "cheatcode": "start", "year": 2010}]
            ]
        ]

        aligned_probs = validate.validate(aligned_records)

        self.assertEqual(len(aligned_probs), len(aligned_records))
        for probs, records in zip(aligned_probs, aligned_records):
            these_probs = list(zip(*probs))[1]
            these_records = list(zip(*records))[1]
            self.assertEqual(len(these_probs), len(these_records))
            for metadatum in these_probs:
                for value in metadatum.values():
                    self.assertIsInstance(value, float)
                    self.assertGreaterEqual(value, 0.0)
                    self.assertLessEqual(value, 1.0)

    def test_full_records(self):
        """Validate a larger set of real records."""
        json_aligned = 'tests/data/1704.01689v1.aligned.json'
        aligned_records = json.load(open(json_aligned))

        aligned_probs = validate.validate(aligned_records)
        self.assertEqual(len(aligned_probs), len(aligned_records))
        for probs, records in zip(aligned_probs, aligned_records):
            these_probs = list(zip(*probs))[1]
            these_records = list(zip(*records))[1]
            self.assertEqual(len(these_probs), len(these_records))
            for metadatum in these_probs:
                for value in metadatum.values():
                    self.assertIsInstance(value, float)
                    self.assertGreaterEqual(value, 0.0)
                    self.assertLessEqual(value, 1.0)
