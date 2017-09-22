import unittest
from unittest import mock

from reflink.process.merge import merge_records
from reflink.process.merge.normalize import filter_records


class TestNormalize(unittest.TestCase):
    """Tests for :func:`reflink.process.merge.normalize.filter_records`."""

    def setUp(self):
        """Given some records..."""
        self.records = [
            ({'foo': 'bar'}, 0.1),
            ({'baz': 'bat'}, 0.4),
            ({'lorem': 'ipsum'}, 0.9)
        ]

    def test_filter_threshold(self):
        """Test that :func:`.filter_records` really does filter by score."""
        self.assertEqual(len(filter_records(self.records, 0.1)[0]), 3)
        self.assertEqual(len(filter_records(self.records, 0.2)[0]), 2)
        self.assertEqual(len(filter_records(self.records, 0.4)[0]), 2)
        self.assertEqual(len(filter_records(self.records, 0.5)[0]), 1)
        self.assertEqual(len(filter_records(self.records, 0.9)[0]), 1)
        self.assertEqual(len(filter_records(self.records, 1.0)[0]), 0)

    def test_filter_threshold_adds_score(self):
        """Each record should be updated with its score."""
        filtered, mean_score = filter_records(self.records, 0.1)
        for record in filtered:
            self.assertIn('score', record)


class TestMergeSimple(unittest.TestCase):
    """Tests for :func:`reflink.process.merge_records` function."""

    def setUp(self):
        """Given aligned references from several extractors, and priors..."""
        self.simple_docs = {
            'ext1': [
                {'source': 'Matthew', 'volume': 'uuddlrlrba', 'year': 2011},
                {'source': 'Erick P', 'volume': 'babaudbalrba', 'year': 2013},
            ],
            'ext2': [
                {'source': 'Matthew', 'volume': 'uuddlrlrbaba', 'year': 2011},
            ],
            'ext3': [
                {'source': 'Johnathan', 'volume': 'start', 'year': 2010},
                {'source': 'Eric Pe', 'volume': 'babaudbalrba', 'year': 2013},
            ]
        }
        self.priors = [
            ('ext1', {'source': 0.9, 'volume': 0.6, 'year': 0.1}),
            ('ext2', {'source': 0.8, 'volume': 0.7, 'year': 0.99}),
            ('ext3', {'source': 0.2, 'volume': 0.9, 'year': 0.001}),
        ]

    @mock.patch('reflink.process.merge.normalize.filter_records')
    @mock.patch('reflink.process.merge.arbitrate.arbitrate_all')
    @mock.patch('reflink.process.merge.beliefs.validate')
    @mock.patch('reflink.process.merge.align.align_records')
    def test_merge_call_pattern(self, mock_align_records, mock_validate,
                                mock_arbitrate_all, mock_filter_records):
        """Test that :func:`.merge_records` calls correct fnx when called."""

        merge_records(self.simple_docs)
        self.assertEqual(mock_align_records.call_count, 1)
        self.assertEqual(mock_validate.call_count, 1)
        self.assertEqual(mock_arbitrate_all.call_count, 1)
        self.assertEqual(mock_filter_records.call_count, 1)

    def test_merge_full(self):
        """Test that :func:`.merge_records` returns a merged reference set."""
        records, score = merge_records(self.simple_docs, self.priors)
        print(records)
        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 3)
        for ref in records:
            self.assertTrue('source' in ref)
            self.assertTrue('volume' in ref)
            self.assertTrue('year' in ref)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)
