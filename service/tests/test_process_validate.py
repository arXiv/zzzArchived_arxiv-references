import json
import unittest

from references.process.merge import beliefs


class TestValidateSimpleRecords(unittest.TestCase):
    """Tests :func:`.beliefs.validate` function with contrived data."""

    def setUp(self):
        """Given some simple aligned records..."""

        self.aligned_records = [
            [
                ["ext1", {"title": "Matt", "cheatcode": "uuddlrlrba",
                          "year": 2011}],
                ["ext2", {"title": "Matt", "cheatcode": "uuddlrlrbaba",
                          "year": 2011}]
            ],
            [
                ["ext1", {"title": "Erick", "cheatcode": "babaudbalrba",
                          "year": 2013}],
                ["ext3", {'title': 'Eric', 'cheatcode': 'babaudbalrba',
                         'year': 2013}]
            ],
            [
                ["ext3", {"title": "John", "cheatcode": "start",
                          "year": 2010}]
            ]
        ]

    def test_simple_records(self):
        """Test that :func:`.validate` returns sensical probabilities."""
        aligned_probs = beliefs.validate(self.aligned_records)

        self.assertEqual(len(aligned_probs), len(self.aligned_records),
                         "Return data should have the same shape as input")
        for probs, records in zip(aligned_probs, self.aligned_records):
            these_probs = list(zip(*probs))[1]
            these_records = list(zip(*records))[1]
            self.assertEqual(len(these_probs), len(these_records),
                             "Return data should have the same shape as input")
            for metadatum in these_probs:
                for value in metadatum.values():
                    self.assertIsInstance(value, float,
                                          "Values should be probs (float)")
                    self.assertGreaterEqual(value, 0.0,
                                            "Probability never less than 0.")
                    self.assertLessEqual(value, 1.0,
                                         "Probability never more than 1.")
    def test_blank_value_is_valid(self):
        """A blank value is treated as a real value."""
        records = [
            [
                ["ext1", {"title": "", "cheatcode": "uuddlrlrba",
                          "year": 2011, "pages": ""}],
                ["ext2", {"title": "Matt", "cheatcode": "uuddlrlrbaba",
                          "year": 2011, "pages": ""}]
            ]
        ]
        aligned_probs = beliefs.validate(records)
        print(aligned_probs)
        self.assertGreater(dict(aligned_probs[0])['ext1']['title'], 0)
        self.assertGreater(dict(aligned_probs[0])['ext1']['pages'], 0)

    def test_missing_value_is_invalid(self):
        """A missing value is treated as invalid."""
        records = [
            [
                ["ext1", {"cheatcode": "uuddlrlrba",
                          "year": 2011}],
                ["ext2", {"title": "Matt", "cheatcode": "uuddlrlrbaba",
                          "year": 2011, "pages": ""}]
            ]
        ]
        aligned_probs = beliefs.validate(records)
        print(aligned_probs)
        self.assertEqual(dict(aligned_probs[0])['ext1'].get('title', 0), 0)
        self.assertEqual(dict(aligned_probs[0])['ext1'].get('pages', 0), 0)


class TestFullRecords(unittest.TestCase):
    """Tests :func:`.beliefs.validate` function with realistic data."""

    def setUp(self):
        """Given some aligned records from a real extraction...."""
        json_aligned = 'tests/data/1704.01689v1.aligned.json'
        with open(json_aligned) as f:
            self.aligned_records = json.load(f)

    def test_full_records(self):
        """Test that :func:`.validate` returns sensical probabilities."""
        aligned_probs = beliefs.validate(self.aligned_records)
        self.assertEqual(len(aligned_probs), len(self.aligned_records),
                         "Return data should have the same shape as input")
        for probs, records in zip(aligned_probs, self.aligned_records):
            these_probs = list(zip(*probs))[1]
            these_records = list(zip(*records))[1]
            self.assertEqual(len(these_probs), len(these_records),
                             "Return data should have the same shape as input")
            for metadatum in these_probs:
                for value in metadatum.values():
                    self.assertIsInstance(value, float,
                                          "Values should be probs (float)")
                    self.assertGreaterEqual(value, 0.0,
                                            "Probability never less than 0.")
                    self.assertLessEqual(value, 1.0,
                                         "Probability never more than 1.")
