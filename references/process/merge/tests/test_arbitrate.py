"""Unit tests for :mod:`references.process.merge.arbitrate`."""

import unittest
from references.domain import Reference
from references.process.merge import arbitrate


class TestArbitrate(unittest.TestCase):
    """Tests for :func:`references.process.merge.arbitrate.arbitrate` function."""

    def test_arbitrate(self):
        """Successful arbitration with valid data."""
        metadata = [
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'volume': 'baz'})
        ]
        valid = [
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1})
        ]
        priors = [
            ('cermine', {'title': 0.8, 'doi': 0.9}),
            ('refextract', {'title': 0.9, 'doi': 0.2, 'volume': 0.2}),
            ('alt', {'title': 0.2})
        ]

        final, score = arbitrate.arbitrate(metadata, valid, priors)
        self.assertIsInstance(final, Reference)
        self.assertEqual(final.title, 'yep')
        self.assertEqual(final.doi, '10.123/123.4566')
        self.assertEqual(final.volume, '12')
        self.assertIsInstance(score, float)
        self.assertGreater(score, 0.5)

    def test_arbitrate_with_list_and_dict(self):
        """Successful arbitration with dict and list values."""
        metadata = [
            ('cermine', {
                'authors': ['yep', 'yep'],
            }),
            ('refextract', {'authors': 'asdf', 'volume': '12'}),
            ('alt', {'authors': 'nope', 'volume': 'baz'})
        ]
        valid = [
            ('cermine', {'authors': 0.9}),
            ('refextract', {'authors': 0.6, 'volume': 0.8}),
            ('alt', {'authors': 0.1})
        ]
        priors = [
            ('cermine', {'authors': 0.8}),
            ('refextract', {'authors': 0.9, 'volume': 0.2}),
            ('alt', {'authors': 0.2})
        ]

        final, score = arbitrate.arbitrate(metadata, valid, priors)
        self.assertIsInstance(final, Reference)
        self.assertEqual(final.authors, ['yep', 'yep'])
        self.assertEqual(final.volume, '12')
        self.assertIsInstance(score, float)
        self.assertGreater(score, 0.0)

    def test_select(self):
        """:func:`.arbitrate._select` returns a sensical score."""
        pooled = {
            'source': {
                'meh': 0.7,
                'yes': 1.5,
                'nope': 0.3
            }
        }
        final, score = arbitrate._select(pooled)
        self.assertEqual(final.source, 'yes')
        # This is low, because we are now scoring partially by completeness.
        self.assertEqual(score, 0.15)

    def test_select_with_ints(self):
        """:func:`.arbitrate._select` works with ``int``s."""
        pooled = {
            'source': {
                'meh': 1,
                'yes': 5,
                'nope': 2
            }
        }
        final, score = arbitrate._select(pooled)
        self.assertEqual(final.source, 'yes')
        # This is low, because we are now scoring partially by completeness.
        self.assertEqual(score, 0.15625)

    def test_similarity_with_strings(self):
        """:func:`.arbitrate._similarity` returns sensical values."""
        self.assertEqual(arbitrate._similarity('meh', 'meh'), 1.0)
        self.assertEqual(arbitrate._similarity('meh', 'meb'), 2/3)
        self.assertEqual(arbitrate._similarity('foo', 'fuzz'), 1/4)

    def test_pool_can_math(self):
        """Test :func:`.arbitrate._pool` can math."""
        def _prob_valid(extractor, field):
            return 0.55 if extractor in ['cermine', 'refextract'] else 0.95

        metadata = [('cermine', {'title': 'meh'}),
                    ('refextract', {'title': 'meh'}),
                    ('alt', {'title': 'too good to be true'})]
        pooled = arbitrate._pool(dict(metadata), ['title'], _prob_valid)
        self.assertEqual(pooled['title']['meh'], 1.1)
        self.assertEqual(pooled['title']['too good to be true'], 0.95)

    def test_pool_handles_list_values(self):
        """:func:`.arbitrate._pool` can handle lists."""
        def _prob_valid(extractor, field):
            return 0.55 if extractor in ['cermine', 'refextract'] else 0.95

        metadata = [('cermine', {'title': ['meh', 'meh']}),
                    ('refextract', {'title': ['meh', 'meh']}),
                    ('alt', {'title': 'too good to be true'})]
        try:
            pooled = arbitrate._pool(dict(metadata), ['title'], _prob_valid)
        except Exception as e:
            self.fail(str(e))

        self.assertEqual(pooled['title'][str(['meh', 'meh'])], 1.1)

    def test_pool_handles_dict_values(self):
        """:func:`.arbitrate._pool` can handle dicts."""
        def _prob_valid(extractor, field):
            return 0.55 if extractor in ['cermine', 'refextract'] else 0.95

        metadata = [('cermine', {'title': {'meh': 'meh'}}),
                    ('refextract', {'title': {'meh': 'meh'}}),
                    ('alt', {'title': 'too good to be true'})]
        try:
            arbitrate._pool(dict(metadata), ['title'], _prob_valid)
        except Exception as e:
            self.fail(str(e))

    def test_pooling_matters(self):
        """Two med-scoring values can beat a single high-scoring value."""
        metadata = [
            ('cermine', {'title': 'meh'}),
            ('refextract', {'title': 'meh'}),
            ('alt', {'title': 'too good to be true'})
        ]
        valid = [
            ('cermine', {'title': 0.5}),
            ('refextract', {'title': 0.6}),
            ('alt', {'title': 1.0})
        ]
        priors = [
            ('cermine', {'title': 1.0}),
            ('refextract', {'title': 1.0}),
            ('alt', {'title': 1.0})
        ]
        final, score = arbitrate.arbitrate(metadata, valid, priors)
        self.assertEqual(final.title, 'meh')
        self.assertLess(score - 0.52, 0.01)

    def test_drop_value_if_prior_missing(self):
        """A field-value is ignored if extractor prior is missing."""
        metadata = [
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'volume': 'baz'})
        ]
        valid = [
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1})
        ]
        priors = [
            ('cermine', {'title': 0.8}),
            ('refextract', {'title': 0.9, 'doi': 0.2, 'volume': 0.2}),
            ('alt', {'title': 0.2})
        ]

        final, score = arbitrate.arbitrate(metadata, valid, priors)
        self.assertEqual(final.doi, 'nonsense')

    def test_misaligned_input_raises_valueerror(self):
        """Misalignment of input raises a ValueError."""
        metadata = [('foo', {}), ('bar', {})]
        valid = [('foo', {}), ('baz', {})]
        priors = [('foo', {}), ('bar', {})]
        with self.assertRaises(ValueError):
            arbitrate.arbitrate(metadata, valid, priors)

        metadata = [('foo', {}), ('bar', {})]
        valid = [('foo', {}), ('bar', {})]
        priors = [('foo', {}), ('baz', {})]
        with self.assertRaises(ValueError):
            arbitrate.arbitrate(metadata, valid, priors)

    def test_arbitrate_all(self):
        """Exercise :func:`references.process.merge.arbitrate.arbitrate_all`."""
        metadata = [[
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4567'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'volume': 'baz'})
        ], [
            ('cermine', {'title': 'yep', 'doi': '10.123/123.4566'}),
            ('refextract', {'title': 'asdf', 'doi': 'nonsense',
                            'volume': '12'}),
            ('alt', {'title': 'nope', 'volume': 'baz'})
        ]]
        valid = [[
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1})
        ], [
            ('cermine', {'title': 0.9, 'doi': 0.8}),
            ('refextract', {'title': 0.6, 'doi': 0.1, 'volume': 0.8}),
            ('alt', {'title': 0.1})
        ]]
        priors = [
            ('cermine', {'title': 0.8, 'doi': 0.9}),
            ('refextract', {'title': 0.9, 'doi': 0.2, 'volume': 0.2}),
            ('alt', {'title': 0.2})
        ]

        final = arbitrate.arbitrate_all(metadata, valid, priors, 3)
        self.assertIsInstance(final, list)
        self.assertEqual(len(final), 2)
        for obj, score in final:
            self.assertGreater(score, 0.0)
            self.assertLess(score, 1.0)
            self.assertIsInstance(obj, Reference)
            self.assertEqual(obj.title, 'yep')
            self.assertEqual(obj.volume, '12')

    def test_empty_value_is_treated_as_value(self):
        """If a key is present, but value empty, treat it as a real value."""
        metadata = [[
            ('cermine', {'title': ""}),
            ('refextract', {'title': ""}),
            ('grobid', {'title': "This is a false positive"})
        ]]
        valid = [[
            ('cermine', {'title': 1.0}),
            ('refextract', {'title': 1.0}),
            ('grobid', {'title': 1.0})
        ]]
        priors = [
            ('cermine', {'title': 1.0}),
            ('refextract', {'title': 1.0}),
            ('grobid', {'title': 1.0}),
        ]
        final, score = arbitrate.arbitrate_all(metadata, valid, priors, 3)[0]
        self.assertEqual(final.title, "")

    def test_missing_value_is_not_treated_as_value(self):
        """If a key is not present, do not treat it as a real value."""
        metadata = [[
            ('cermine', {}),
            ('refextract', {}),
            ('grobid', {'title': "This is correct"})
        ]]
        valid = [[
            ('cermine', {'title': 1.0}),
            ('refextract', {'title': 1.0}),
            ('grobid', {'title': 1.0})
        ]]
        priors = [
            ('cermine', {'title': 1.0}),
            ('refextract', {'title': 1.0}),
            ('grobid', {'title': 1.0}),
        ]
        final, score = arbitrate.arbitrate_all(metadata, valid, priors, 3)[0]
        self.assertEqual(final.title, "This is correct")
