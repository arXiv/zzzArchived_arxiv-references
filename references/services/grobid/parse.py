"""Business logic for processing Grobid extracted references."""

import re
import io
import xml.etree.ElementTree
from xml.etree.ElementTree import Element
from typing import List, Dict

from arxiv.base import logging
from references.domain import Reference

logger = logging.getLogger(__name__)

RE_XMLNS = re.compile(r'^[{](.*?)[}].*')
XMLNS = 'http://www.tei-c.org/ns/1.0'


def _xml_set_ns(root: Element) -> None:
    global XMLNS
    XMLNS = RE_XMLNS.findall(root.tag)[0]


def _xml_tag(tag: str) -> str:
    return '{{{xmlns}}}{tag}'.format(xmlns=XMLNS, tag=tag)


def _xml_path_elem(elem: Element, path: str) -> List[Element]:
    path = '/'.join([xt(i) for i in path.split('/')])
    return elem.findall(path)


def _xml_path_attr(elem: Element, path: str, attrib: str) -> str:
    found = _xml_path_elem(elem, path)
    return ' '.join([
        f.attrib.get(attrib, '') for f in found if f is not None
    ])


def _xml_path_text(elem: Element, path: str) -> str:
    found = _xml_path_elem(elem, path)
    return ' '.join([
        f.text for f in found if f is not None and f.text is not None
    ])


xt = _xml_tag


def _xml_format_biblStruct(bbl: Element) -> dict:
    """
    Given a TEI biblStruct, format the reference to our schema.

    Note that once again, the extraction process seems to have trouble.
    Therefore, we must case out the presence of <analytic> and <monogr>
    sections, straying from the strict definitions in TEI.

    Parameters
    ----------
    bbl : xml.etree.ElementTree
        A particular part of the xml tree corresponding to a TEI:biblStruct

    Returns
    -------
    reference_metadata : dict
        A single schema formatted reference line
    """
    def _authors(bbl: Element, path: str) -> List[Dict[str, str]]:
        authors = []
        for elem in _xml_path_elem(bbl, path):
            first = ' '.join(map(lambda x: x.text, elem.iter(xt('forename'))))  # type: ignore
            last = ' '.join(map(lambda x: x.text, elem.iter(xt('surname'))))  # type: ignore
            auth = {
                'givennames': first,
                'surname': last
            }
            authors.append(auth)
        return authors

    if _xml_path_elem(bbl, 'analytic'):
        # we have an article that is part of a collection or journal
        authors = _authors(bbl, 'analytic/author')
        title = _xml_path_text(bbl, 'analytic/title')
        source = _xml_path_text(bbl, 'monogr/title')
    elif _xml_path_elem(bbl, 'monogr'):
        # we have a book or mis-labelled article
        authors = _authors(bbl, 'monogr/author')
        title = _xml_path_text(bbl, 'monogr/title')
        source = _xml_path_text(bbl, 'monogr/imprint/publisher')

    # no matter what, these values come the monogr section
    year = _xml_path_attr(
        bbl, 'monogr/imprint/date[@type="published"]', 'when'
    )
    pages = '{}-{}'.format(
        _xml_path_attr(bbl, 'monogr/imprint/biblScope[@unit="page"]', 'from'),
        _xml_path_attr(bbl, 'monogr/imprint/biblScope[@unit="page"]', 'to')
    )
    volume = _xml_path_text(bbl, 'monogr/imprint/biblScope[@unit="volume"]')
    issue = _xml_path_text(bbl, 'monogr/imprint/biblScope[@unit="issue"]')

    if pages == '-':
        pages = ''

    return {
        'authors': authors,
        'title': title,
        'year': year,
        'pages': pages,
        'source': source,
        'volume': volume,
        'issue': issue,
    }


def format_grobid_output(output: bytes) -> List[Reference]:
    """
    Transform GROBID output to internal metadata struct.

    Take the output of GROBID and return the metadata in the format expected by
    the references schema. For a description of TEI, Text Encoding Initiative
    (the format of the XML), see the documentation on the website
    (particularly, the bibliography section):

    http://www.tei-c.org/release/doc/tei-p5-doc/en/html/ref-biblStruct.html

    Parameters
    ----------
    output : dict
        The output of the GROBID API call, structured dict of metadata

    Returns
    -------
    metadata : list
        List of reference metadata (dict) conforming to references schema.
    """
    filestring = io.StringIO(output.decode('utf-8'))
    root = xml.etree.ElementTree.parse(filestring).getroot()
    _xml_set_ns(root)

    # make sure we are only dealing with the final reference list
    try:
        listbbl = list(root.iter(tag=xt('listBibl')))[0]
    except IndexError:
        msg = 'GROBID output does not contain references'
        logger.error(msg)
        raise IndexError(msg)

    blank_reference = {
        'identifiers': [{'identifier_type': '', 'identifier': ''}],
        'raw': '', 'volume': '', 'issue': '', 'pages': '', 'reftype': '',
        'doi': '', 'authors': [], 'title': '', 'year': '', 'source': '',
    }

    # ========================================================================
    # iterate over the references in that list
    references = []
    for bbl in listbbl.iter(tag=xt('biblStruct')):
        reference = dict(blank_reference)
        reference.update(_xml_format_biblStruct(bbl))
        references.append(Reference(**reference))   # type: ignore

    return references
