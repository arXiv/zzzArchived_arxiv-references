import unittest
from reflink.process.merge import arbitrate


class TestArbitrate(unittest.TestCase):
    """Tests for :func:`reflink.process.merge.arbitrate.arbitrate` function."""

    def test_arbitrate(self):
        """Test successful arbitration with valid data."""
        metadata = [
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'foo': 'bar', 'volume': 'baz'})
        ]
        valid = [
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1, 'foo': 1.0})
        ]
        priors = [
            ('cermine', {'title': 0.8, 'doi': 0.9}),
            ('refextract', {'title': 0.9, 'doi': 0.2, 'volume': 0.2}),
            ('alt', {'title': 0.2, 'foo': 0.9})
        ]

        final, score = arbitrate.arbitrate(metadata, valid, priors)
        self.assertIsInstance(final, dict)
        self.assertEqual(final['title'], 'yep')
        self.assertEqual(final['doi'], '10.123/123.4566')
        self.assertEqual(final['volume'], '12')
        self.assertEqual(final['foo'], 'bar')
        self.assertIsInstance(score, float)
        self.assertLess(score - 0.625, 0.0001)

    def test_drop_value_if_prior_missing(self):
        """Test that a field-value is ignored if extractor prior is missing."""
        metadata = [
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'foo': 'bar', 'volume': 'baz'})
        ]
        valid = [
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1, 'foo': 1.0})
        ]
        priors = [
            ('cermine', {'title': 0.8}),
            ('refextract', {'title': 0.9, 'doi': 0.2, 'volume': 0.2}),
            ('alt', {'title': 0.2, 'foo': 0.9})
        ]

        final, score = arbitrate.arbitrate(metadata, valid, priors)
        self.assertEqual(final['doi'], 'nonsense')

    def test_misaligned_input_raises_valueerror(self):
        """Test that misalignment of input is caught."""
        metadata = [('foo', {}), ('bar', {})]
        valid = [('foo', {}), ('baz', {})]
        priors = [('foo', {}), ('bar', {})]
        with self.assertRaises(ValueError):
            final = arbitrate.arbitrate(metadata, valid, priors)

        metadata = [('foo', {}), ('bar', {})]
        valid = [('foo', {}), ('bar', {})]
        priors = [('foo', {}), ('baz', {})]
        with self.assertRaises(ValueError):
            final = arbitrate.arbitrate(metadata, valid, priors)

    def test_arbitrate_all(self):
        """Exercise :func:`reflink.process.merge.arbitrate.arbitrate_all`."""

        metadata = [[
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'foo': 'bar', 'volume': 'baz'})
        ],[
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'foo': 'bar', 'volume': 'baz'})
        ]]
        valid = [[
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1, 'foo': 1.0})
        ], [
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1, 'foo': 1.0})
        ]]
        priors = [
            ('cermine', {'title': 0.8, 'doi': 0.9}),
            ('refextract', {'title': 0.9, 'doi': 0.2, 'volume': 0.2}),
            ('alt', {'title': 0.2, 'foo': 0.9})
        ]

        final = arbitrate.arbitrate_all(metadata, valid, priors)
        self.assertIsInstance(final, list)
        for obj, score in final:
            self.assertIsInstance(obj, dict)
            self.assertEqual(obj['title'], 'yep')
            self.assertEqual(obj['doi'], '10.123/123.4566')
            self.assertEqual(obj['volume'], '12')
            self.assertEqual(obj['foo'], 'bar')
