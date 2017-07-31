import unittest
from unittest import mock

from reflink.process.merge import merge

simple_docs = {
    'ext1': [
        {'title': 'Matt', 'cheatcode': 'uuddlrlrba', 'year': 2011},
        {'title': 'Erick', 'cheatcode': 'babaudbalrba', 'year': 2013},
    ],
    'ext2': [
        {'title': 'Matt', 'cheatcode': 'uuddlrlrbaba', 'year': 2011},
    ],
    'ext3': [
        {'title': 'John', 'cheatcode': 'start', 'year': 2010},
        {'title': 'Eric', 'cheatcode': 'babaudbalrba', 'year': 2013},
    ]
}

priors = [
    (
        'ext1',
        {
            'title': 0.9,
            'cheatcode': 0.6,
            'year': 0.1
        }
    ),
    (
        'ext2',
        {
            'title': 0.8,
            'cheatcode': 0.7,
            'year': 0.99
        }
    ),
    (
        'ext3',
        {
            'title': 0.2,
            'cheatcode': 0.9,
            'year': 0.001
        }
    ),
]


class TestMerge(unittest.TestCase):
    """Tests for :func:`reflink.process.merge.merge.merge_records` function."""

    @mock.patch('reflink.process.merge.normalize.filter_records')
    @mock.patch('reflink.process.merge.arbitrate.arbitrate_all')
    @mock.patch('reflink.process.merge.validate.validate')
    @mock.patch('reflink.process.merge.align.align_records')
    def test_merge_call_pattern(self, mock_align_records, mock_validate,
                                mock_arbitrate_all, mock_filter_records):
        """Test that :func:`.merge.merge_records` calls correct fnx."""

        merge.merge_records(simple_docs)
        self.assertEqual(mock_align_records.call_count, 1)
        self.assertEqual(mock_validate.call_count, 1)
        self.assertEqual(mock_arbitrate_all.call_count, 1)
        self.assertEqual(mock_filter_records.call_count, 1)

    def test_merge_full(self):
        """Perform the entire merge stack."""
        records, score = merge.merge_records(simple_docs, priors)
        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 3)
        for ref in records:
            self.assertTrue('title' in ref)
            self.assertTrue('cheatcode' in ref)
            self.assertTrue('year' in ref)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)
