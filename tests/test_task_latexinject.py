import sys
sys.path.append('.')

import os
import json
import unittest
from unittest.mock import patch

from reflink.process.inject import latexinjector

data_directory = 'tests/data'


def revfile(ext):
    revtex_files = os.path.join(data_directory, 'revtex-article')
    return '{}.{}'.format(revtex_files, ext)


class TestLatexInjection(unittest.TestCase):
    def test_encoding_tex(self):
        src = revfile('tex')
        self.assertEqual(latexinjector.detect_encoding(src), 'ascii')

    def test_encoding_cermine(self):
        src = os.path.join(data_directory, '1702.07336.cermxml')
        self.assertEqual(latexinjector.detect_encoding(src), 'ascii')

    def test_bib_element_head(self):
        src = revfile('bbl')
        ans = revfile('bbl-head')
        with open(src) as fn, open(ans) as res:
            content = fn.read()
            answer = res.read().strip()
            self.assertEqual(latexinjector.bib_items_head(content), answer)

    def test_bib_element_length(self):
        src = revfile('bbl')
        with open(src) as fn:
            content = fn.read()
            self.assertEqual(
                len(list(latexinjector.bib_items_iter(content))), 5
            )

    def test_bib_element_tail(self):
        src = revfile('bbl')
        ans = revfile('bbl-tail')
        with open(src) as fn, open(ans) as res:
            content = fn.read()
            answer = res.read().strip()
            self.assertEqual(latexinjector.bib_items_tail(content), answer)

    def test_url_replacement(self):
        reference = {
            'document': 'foo',
            'identifier': 'bar',
        }

        baseurl = 'https://arxiv.org/references'
        self.assertEqual(
            latexinjector.url_formatter_arxiv(reference, baseurl=baseurl),
            '\\href{%s/foo/ref/bar/resolve}{GO}' % (baseurl)
        )

    def test_bibitem_reference_match(self):
        file_ref = revfile('cermxml.json')
        file_bbl = revfile('bbl')
        reference_lines = [r['raw'] for r in json.load(open(file_ref))]
        bibitems = list(latexinjector.bib_items_iter(open(file_bbl).read()))

        ind = latexinjector.match_by_cost(
            latexinjector.cleaned_bib_entries(bibitems),
            latexinjector.cleaned_reference_lines(reference_lines),
        )
        self.assertEqual(ind, [0, 1, 2, 3, 4])

    def test_inject_urls(self):
        file_ref = revfile('cermxml.json')
        file_pdf = revfile('pdf')
        file_src = revfile('tar.gz')
        metadata = json.load(open(file_ref))

        with patch('reflink.process.inject.latexinjector.inject_urls') as injectfunc:
            injectfunc.return_value = 'new_pdf_name.pdf'
            self.assertEqual(
                latexinjector.inject_urls(file_pdf, file_src, metadata),
                'new_pdf_name.pdf'
            )

if __name__ == '__main__':
    unittest.main()
