import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

import re
import os
import shutil
import subprocess
import xml.etree.ElementTree
from contextlib import contextmanager

from reflink.tasks import util

class ExtractionError(Exception):
    pass

def _cxml_format_reference_line(elm):
    """
    Convert a CERMINE XML element to a reference line i.e.
    Bierbaum, Matt and Pierson, Erick arxiv:1706.0000
    """
    # regex for cleaning up the extracted reference lines a little bit:
    #  1. re_multispace -- collapse 2+ spaces into a single one
    #  2. re_numbering -- remove numbers at beginning of line matching 1., 1, [1], (1)
    re_multispace = re.compile(r"\s{2,}")
    re_numbering = re.compile(r'^([\[\(]?\d+[\]\)]?\.?)(.*)')

    if len(elm) == 0:
        return elm.text.strip()

    children = ' '.join([_cxml_format_reference_line(i) for i in elm])
    out = '{} {}'.format(elm.text.strip(), children)
    out = re_multispace.subn(' ', out)[0].strip()
    out = re_numbering.sub(r'\2', out).strip()
    return out

def _cxml_element_func(tagname):
    """
    Return the a function which retrieves the text element associated with a
    certain xml tag from an xml root element.
    """
    def _inner(root):
        return ' '.join([i.text.strip() for i in root.iter(tag=tagname)])
    return _inner

def _cxml_authors(root):
    """
    Given an xml element return the marked up information corresponding to
    patterns that look like CERMINE authors.
    """
    authors = []
    
    firstname = _cxml_element_func('given-names')
    lastname = _cxml_element_func('surname')

    for auth in root.iter(tag='string-name'):
        authors.append({'givenname': firstname(auth), 'surname': lastname(auth)})
    return authors

def _cxml_format_document(root):
    """
    Convert a CERMINE XML element into a document i.e.
    {"author": {"givenname": "Matt", "surname", "Bierbaum"}, "journal": "arxiv"}
    """
    constructor = {
        'authors': _cxml_authors,
        'article-title': _cxml_element_func('article-title'),
        'journal': _cxml_element_func('source'),
        'year': _cxml_element_func('year'),
        'volume': _cxml_element_func('volume'),
        'page': _cxml_element_func('fpage'),
    }

    return {
        key: func(root) for key, func in constructor.items()
    }

def cermine_parse_xml(filename: str) -> dict:
    """
    Transforms a CERMINE XML file into human and machine readable references:
        1. Reference lines i.e. the visual form in the paper
        2. JSON documents with separated metadata

    See :func:`cermine_extract_references` for parameters 
    """
    root = xml.etree.ElementTree.parse(filename).getroot()

    refs = list(root.iter(tag='ref'))
    lines = [_cxml_format_reference_line(ref) for ref in refs]
    docs = [_cxml_format_document(ref) for ref in refs]
    return lines, docs

def cermine_extract_references(filename: str, cleanup: bool = True) -> str:
    """
    Copy the pdf to a temporary directory, run CERMINE and return the extracted
    references as a string. Cleans up all temporary files.

    Parameters
    ----------
    filename : str
        Name of the pdf from which to extract references

    cleanup : bool [True]
        Whether to delete intermediate files afterward.

    Returns
    -------
    reference_lines : list of strings
        Simplified reference lines as extracted from the pdf (no markup, true
        to visual representation in the PDF)

    reference_docs : list of dicts
        Dictionary of reference metadata with metadata separated into author,
        journal, year, etc
    """
    filename = os.path.abspath(filename)
    fldr, name = os.path.split(filename)
    stub, ext = os.path.splitext(os.path.basename(filename))

    with util.tempdir(cleanup=cleanup) as tmpdir:
        # copy the pdf to our temporary location
        tmppdf = os.path.join(tmpdir, name)
        shutil.copyfile(filename, tmppdf)

        try:
            # FIXME: magic string for cermine container
            util.run_docker('mattbierbaum/cermine', [[tmpdir, '/pdfs']])
        except subprocess.CalledProcessError as exc:
            logger.error(
                'CERMINE failed to extract references for {}'.format(filename)
            )
            raise ExtractionError(filename) from exc

        cxml = os.path.join(tmpdir, '{}.cermxml'.format(stub))
        if not os.path.exists(cxml):
            logger.error(
                'CERMINE produced no output metadata for {}'.format(filename)
            )
            raise IOError('{} not found, expected as output'.format(cxml))

        return cermine_parse_xml(cxml)

