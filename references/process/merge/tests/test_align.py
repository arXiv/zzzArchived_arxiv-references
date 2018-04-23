import json
import unittest

from references.domain import Reference
from references.process.merge import align

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
                Reference(title='Matt', year=2011),
                Reference(title='Erick', year=2013),
            ],
            'ext2': [
                Reference(title='Matt', year=2011),
            ],
            'ext3': [
                Reference(title='John', year=2010),
                Reference(title='Eric', year=2013),
            ]
        }

        aligned_answer = [
            [
                ["ext1", Reference(title='Matt', year=2011)],
                ["ext2", Reference(title='Matt', year=2011)]
            ],
            [
                ["ext1", Reference(title='Erick', year=2013)],
                ["ext3", Reference(title='Eric', year=2013)]
            ],
            [
                ["ext3", Reference(title='John', year=2010)]
            ]
        ]

        aligned_calc = align.align_records(docs)
        for ref_ans, ref_calc in zip(aligned_answer, aligned_calc):
            self.assertDictEqual(dict(ref_ans), dict(ref_calc))
