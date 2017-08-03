"""Business logic for processing Cermine extracted references."""

import regex as re
import os
import shutil
import subprocess
import xml.etree.ElementTree

from reflink import types
from reflink import logging
from reflink.process import util
from reflink.process.extract import regex_identifiers
from reflink.services import Cermine


logger = logging.getLogger(__name__)


def _cxml_element_func(tagname):
    """
    Return a function which retrieves the text element associated with a
    certain xml tag from an xml root element.

    Can be used like a partial:

    .. code-block:: python

        func = _cxml_element_func(tagname='country')
        countries = func(xmlroot)

    Returns
    -------
    func : callable
    """
    def _inner(root):
        return ' '.join([i.text.strip() for i in root.iter(tag=tagname)])
    return _inner


def _cxml_ref_authors(ref):
    """
    Given an xml element return the marked up information corresponding to
    patterns that look like CERMINE authors. `ref` is the root of the reference
    in the xml.
    """
    authors = []

    firstname = _cxml_element_func('given-names')
    lastname = _cxml_element_func('surname')

    for auth in ref.iter(tag='string-name'):
        authors.append(
            {
                'givennames': firstname(auth),
                'surname': lastname(auth),
                'prefix': '',
                'suffix': ''
            }
        )
    return authors


def _cxml_format_reference_line(elm):
    """
    Convert a CERMINE XML element to a reference line i.e.:

        Bierbaum, Matt and Pierson, Erick arxiv:1706.0000

    Parameters
    ----------
    elm : xml.etree.ElementTree
        reference xml root from CERMINE

    Returns
    -------
    line : str
        The formatted reference line as seen in the PDF
    """

    # regex for cleaning up the extracted reference lines a little bit:
    #  1. re_multispace -- collapse 2+ spaces into a single one
    #  2. re_numbering -- remove numbers at beginning of line matching:
    #       1., 1, [1], (1)
    #  3. re_punc_spaces_left -- cermxml doesn't properly format the tags
    #       (adds too many spaces). so lets try to get rid of the obvious
    #       ones like ' ,' ' )' ' .'
    #  4. re_punc_spaces_right -- same on the other side
    #  5. re_arxiv_colon -- a big thing we are trying to extract (ids) gets
    #       mangled by cermine as well. try to fix it as well
    re_multispace = re.compile(r"\s{2,}")
    re_numbering = re.compile(r'^([[(]?\d+[])]?\.?)(.*)')
    re_punc_spaces_left = re.compile(r'\s([,.)])')
    re_punc_spaces_right = re.compile(r'([(])\s')
    re_arxiv_colon = re.compile(r'((?i:arxiv\:))\s+')
    re_trailing_punc = re.compile(r"[,.]$")

    text = ' '.join([
        txt.strip() for txt in elm.itertext()
    ])
    text = text.strip()
    text = re_multispace.subn(' ', text)[0].strip()
    text = re_numbering.sub(r'\2', text).strip()
    text = re_punc_spaces_left.subn(r'\1', text)[0].strip()
    text = re_punc_spaces_right.subn(r'\1', text)[0].strip()
    text = re_arxiv_colon.subn(r'\1', text)[0].strip()
    text = re_trailing_punc.sub('', text)
    return text


def cxml_format_document(root, documentid=''):
    """
    Convert a CERMINE XML element into a reference document i.e.:

        {
            "author": {"givenname": "Matt", "surname", "Bierbaum"},
            "journal": "arxiv",
            "article-title": "Some bad paper",
            "year": 2017,
            "volume": 1,
            "page": 1
        }

    Parameters
    ----------
    root : xml.etree.ElementTree
        reference xml root from CERMINE

    Returns
    -------
    doc : dictionary
        Formatted reference document using CERMINE metadata
    """
    reference_constructor = {
        'authors': _cxml_ref_authors,
        'raw': _cxml_format_reference_line,
        'title': _cxml_element_func('article-title'),
        'source': _cxml_element_func('source'),
        'year': _cxml_element_func('year'),
        'volume': _cxml_element_func('volume'),
        'pages': _cxml_element_func('fpage'),
        'issue': _cxml_element_func('issue'),
    }

    # things that cermine does not extract / FIXME -- get these somehow?!
    unknown_properties = {
        'identifiers': [{'identifier_type': '', 'identifier': ''}],
        'reftype': '',
        'doi': ''
    }

    references = []
    for refroot in root.iter(tag='ref'):
        reference = {
            key: func(refroot) for key, func in reference_constructor.items()
        }
        reference.update(unknown_properties)

        # add regex extracted information to the metadata (not CERMINE's)
        rawline = reference.get('raw', '') or ''
        identifiers = regex_identifiers.extract_identifiers(rawline)
        reference.update(identifiers)

        references.append(reference)

    return references


def convert_cxml_json(raw_data: str, document_id: str) -> dict:
    """
    Transforms a CERMINE XML file into human and machine readable references:
        1. Reference lines i.e. the visual form in the paper
        2. JSON documents with separated metadata

    Parameters
    ----------
    raw_data : str
        Raw XML response from Cermine.
    document_id : str

    Returns
    -------
    see :func:`cermine_extract_references`
    """
    root = xml.etree.ElementTree.fromstring(raw_data)
    return cxml_format_document(root, document_id)


def extract_references(filename: str, document_id: str,
                       cleanup: bool = True) -> types.ReferenceMetadata:
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
    reference_docs : list of dicts
        Dictionary of reference metadata with metadata separated into author,
        journal, year, etc
    """
    filename = os.path.abspath(filename)

    if not os.path.exists(filename):
        logger.error("{} does not exist".format(filename))
        raise FileNotFoundError(filename)

    cermine = Cermine()

    try:
        data = cermine.session.extract_references(filename)
    except IOError as e:
        msg = 'Cermine extraction failed for %s: %s' % (filename, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    return convert_cxml_json(data, document_id)
