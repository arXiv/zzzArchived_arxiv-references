"""Tests for :mod:`references.process.orchestrate` module."""

import unittest

from references.process.merge import beliefs


class TestBeliefs(unittest.TestCase):
    def test_is_integer(self):
        self.assertEqual(beliefs.is_integer('209a'), 0.0)
        self.assertEqual(beliefs.is_integer('209'), 1.0)

    def test_is_integer_like(self):
        self.assertEqual(beliefs.is_integer_like('209 4'), 0.8)
        self.assertEqual(beliefs.is_integer_like('209 4.0000'), 0.4)

    def test_is_year(self):
        self.assertEqual(beliefs.is_year('2010'), 1.0)
        self.assertEqual(beliefs.is_year('010'), 0.0)

    def test_is_year_like(self):
        self.assertEqual(beliefs.is_year_like('blah 2010'), 1.0)
        self.assertEqual(beliefs.is_year_like('blah 201'), 0.0)

    def test_is_pages(self):
        self.assertEqual(beliefs.is_pages('20 - 30'), 1.0)
        self.assertEqual(beliefs.is_pages('20 - 10'), 0.5)
        self.assertEqual(beliefs.is_pages('20 - '), 0.0)

    def test_valid_doi(self):
        self.assertEqual(beliefs.valid_doi('doi:10.1002/0470841559.ch1'), 1.0)
        self.assertEqual(beliefs.valid_doi('DOX 10.1002/0470841559.ch1'), 0.0)

    def test_valid_identifier_arxiv(self):
        self.assertEqual(beliefs.valid_arxiv_id('arxiv:1703.03442'), 1.0)
        self.assertEqual(beliefs.valid_arxiv_id('arxix:1703.03442'), 0.0)

    def test_valid_identifier_isbn(self):
        self.assertEqual(
            beliefs.valid_identifier(
                [{
                    'identifier_type': 'isbn',
                    'identifier': 'ISBN 978-3-16-148410-0'
                }]
            ), 1.0
        )
        self.assertEqual(
            beliefs.valid_identifier(
                [{
                    'identifier_type': 'isbn',
                    'identifier': 'ISSN 978-3-16-148410-0'
                }]
            ), 0.0
        )
