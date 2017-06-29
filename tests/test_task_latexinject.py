import sys
sys.path.append('.')

import os
import json
import jsonschema
import subprocess
import unittest
from unittest import mock

from reflink.services import data_store
from reflink.process.inject import latexinjector

data_directory = 'tests/data'
revtex_files = os.path.join(data_directory, 'revtex-article')

class TestLatexInjection(unittest.TestCase):
    def test_encoding_tex(self):
        src = '{}.tex'.format(revtex_files)
        self.assertEqual(latexinjector.detect_encoding(src), 'ascii')

    def test_encoding_cermine(self):
        src = os.path.join(data_directory, '1702.07336.cermxml')
        self.assertEqual(latexinjector.detect_encoding(src), 'ascii')

    def test_bib_element_head(self):
        src = '{}.bbl'.format(revtex_files)
        ans = '{}.bbl-head'.format(revtex_files)
        with open(src) as fn, open(ans) as res:
            content = fn.read()
            answer = res.read().strip()
            self.assertEqual(latexinjector.bib_items_head(content), answer)

    def test_bib_element_length(self):
        src = '{}.bbl'.format(revtex_files)
        with open(src) as fn:
            content = fn.read()
            self.assertEqual(
                len(list(latexinjector.bib_items_iter(content))), 5
            )

    def test_bib_element_tail(self):
        src = '{}.bbl'.format(revtex_files)
        ans = '{}.bbl-tail'.format(revtex_files)
        with open(src) as fn, open(ans) as res:
            content = fn.read()
            answer = res.read().strip()
            self.assertEqual(latexinjector.bib_items_tail(content), answer)

    def test_url_replacement(self):
        reference_line = 'matt & erick'
        self.assertEqual(
            latexinjector.url_formatter_arxiv(reference_line),
            '\\href{https://arxiv.org/lookup?q=matt+\\%26+erick}{GO}'
        )

    def test_bibitem_reference_match(self):
        file_ref = '{}.cermxml.json'.format(revtex_files)
        file_bbl = '{}.bbl'.format(revtex_files)
        reference_lines = [r['raw'] for r in json.load(open(file_ref))]
        bibitems = list(latexinjector.bib_items_iter(open(file_bbl).read()))

        ind = latexinjector.match_by_cost(
            latexinjector.cleaned_bib_entries(bibitems),
            latexinjector.cleaned_reference_lines(reference_lines),
        )
        self.assertEqual(ind, [0,1,2,3,4])


if __name__ == '__main__':
    unittest.main()
