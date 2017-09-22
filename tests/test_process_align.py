import json
import unittest

from reflink.process.merge import align

testfile = 'tests/data/1704.01689v1'
extensions = ['.cermine.json', '.grobid.json', '.scienceparse-formatted.json']
labels = ['cermine', 'grobid', 'scienceparse']
json_aligned = '{}.aligned.json'.format(testfile)


def obj_digest(obj):
    return json.dumps(obj, sort_keys=True, indent=2)


class TestAlignRecords(unittest.TestCase):
    def test_simple_records(self):
        """Regression test for alignment with fake records."""
        docs = {
            'ext1': [
                {'name': 'Matt', 'cheatcode': 'uuddlrlrba', 'year': 2011},
                {'name': 'Erick', 'cheatcode': 'babaudbalrba', 'year': 2013},
            ],
            'ext2': [
                {'name': 'Matt', 'cheatcode': 'uuddlrlrbaba', 'year': 2011},
            ],
            'ext3': [
                {'name': 'John', 'cheatcode': 'start', 'year': 2010},
                {'name': 'Eric', 'cheatcode': 'babaudbalrba', 'year': 2013},
            ]
        }

        aligned_answer = [
            [
                ["ext1", {"name": "Matt", "cheatcode": "uuddlrlrba",
                          "year": 2011}],
                ["ext2", {"name": "Matt", "cheatcode": "uuddlrlrbaba",
                          "year": 2011}]
            ],
            [
                ["ext1", {"name": "Erick", "cheatcode": "babaudbalrba",
                          "year": 2013}],
                ["ext3", {'name': 'Eric', 'cheatcode': 'babaudbalrba',
                          'year': 2013}]
            ],
            [
                ["ext3", {"name": "John", "cheatcode": "start", "year": 2010}]
            ]
        ]

        aligned_calc = align.align_records(docs)
        self.assertEqual(obj_digest(aligned_calc), obj_digest(aligned_answer))

    def test_full_records(self):
        """Regression test for alignment with real data."""
        records = [json.load(open(testfile+ext)) for ext in extensions]
        recs = {lbl: rec for lbl, rec in zip(labels, records)}
        aligned_calc = align.align_records(recs)
        aligned_file = json.load(open(json_aligned))
        with open('ack.json', 'w') as f:
            json.dump(aligned_calc, f)
        self.assertEqual(obj_digest(aligned_calc), obj_digest(aligned_file))
