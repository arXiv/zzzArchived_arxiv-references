"""Business logic for processing Cermine extracted references."""

import os
from io import BytesIO
import shutil
import subprocess
import xml.etree.ElementTree as ET
from typing import List, Callable, Dict

import regex as re

from arxiv.base import logging
from references.util import regex_identifiers
# from references.services import cermine
from references.domain import Reference, Identifier


logger = logging.getLogger(__name__)


def _cxml_element_func(tagname: str) -> Callable:
    """
    Generate a function to extract text from an XML element.

    Returns a function which retrieves the text element associated with a
    certain xml tag from an xml root element.

    Can be used like a partial:

    .. code-block:: python

        func = _cxml_element_func(tagname='country')
        countries = func(xmlroot)

    Returns
    -------
    func : callable
    """
    def _inner(root):   # type: ignore
        return ' '.join([i.text.strip() for i in root.iter(tag=tagname)])
    return _inner


def _cxml_ref_authors(ref: ET.Element) -> List[dict]:
    """
    Extract author metadata from a reference element.

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


def _cxml_format_reference_line(elm: ET.Element) -> str:
    """
    Convert a CERMINE XML element to a reference line.

    For example:

        Bierbaum, Matt and Pierson, Erick arxiv:1706.0000

    Parameters
    ----------
    elm : ET
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


def cxml_format_document(root: ET.Element) -> List[Reference]:
    """
    Convert a CERMINE XML element into a reference document.

    For example:

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
    root : ET
        reference xml root from CERMINE

    Returns
    -------
    doc : dictionary
        Formatted reference document using CERMINE metadata
    """
    reference_constructor: Dict[str, Callable] = {
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
    # unknown_properties = {
    #     'identifiers': [{'identifier_type': '', 'identifier': ''}],
    #     'reftype': '',
    #     'doi': ''
    # }

    references = []
    for refroot in root.iter(tag='ref'):
        reference = {
            key: func(refroot) for key, func in reference_constructor.items()
        }

        # add regex extracted information to the metadata (not CERMINE's)
        rawline = reference.get('raw', '') or ''
        partial = regex_identifiers.extract_identifiers(rawline)

        reference['identifiers'] = [
            Identifier(**ident)     # type: ignore
            for ident in reference.get('identifiers', [])
        ]
        reference['identifiers'] += partial.identifiers
        references.append(Reference(**reference))  # type: ignore

    return references


def cxml_to_json(raw_data: bytes) -> List[Reference]:
    """
    Transforms a CERMINE XML file into internal reference struct.

    Parameters
    ----------
    raw_data : bytes
        Raw XML response from Cermine.
    document_id : str

    Returns
    -------
    see :func:`cermine_extract_references`
    """
    return cxml_format_document(ET.parse(BytesIO(raw_data)).getroot())
