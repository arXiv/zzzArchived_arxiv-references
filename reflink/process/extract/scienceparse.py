"""Business logic for processing ScienceParse extracted references."""

import os

from reflink import logging
from reflink import types
from reflink.status import HTTP_200_OK
from reflink.services.scienceparse import scienceParse

logger = logging.getLogger(__name__)


def parse_auth_line(text):
    """
    Very simple attempt to split an author line into first / last names.
    At the moment, this only uses spaces, but could be more involved later
    """
    words = text.split(' ')
    first, last = words[:-1], words[-1:]
    return [' '.join(part) for part in [first, last]]


def format_scienceparse_output(output: dict) -> types.ReferenceMetadata:
    """
    Take the output of ScienceParse and return the metadata in the format
    expected by the reflink schema

    Parameters
    ----------
    output : dict
        The output of the ScienceParse API call, structured dict of metadata

    Returns
    -------
    metadata : types.ReferenceMetadata
        List of reference metadata conforming to reflink schema
    """
    if 'references' not in output:
        msg = 'ScienceParse output does not contain references'
        logger.error(msg)
        raise KeyError(msg)

    unknown_properties = {
        'identifiers': [{'identifier_type': '', 'identifier': ''}],
        'raw': '',
        'volume': '',
        'issue': '',
        'pages': '',
        'reftype': '',
        'doi': ''
    }

    references = []
    for ref in output['references']:
        authors = []
        for auth in ref.get('authors', []):
            if auth:
                authors.append(parse_auth_line(auth))
        authors = [
            {
                'givennames': first,
                'surname': last
            } for first, last in authors
        ]
        newform = {
            'title': ref.get('title'),
            'year': str(ref.get('year')),
            'source': ref.get('venue'),
            'authors': authors
        }
        references.append(dict(newform, **unknown_properties))

    return references


def extract_references(filename: str, document_id: str) -> types.ReferenceMetadata:
    """
    Extract references from the PDF at ``filename`` using ScienceParse.

    Return the reponse formatted to the schema for all references

    Parameters
    ----------
    filename : str
        Name of the pdf from which to extract references

    Returns
    -------
    reference_docs : list
        Dictionary of reference metadata with metadata separated into author,
        journal, year, etc
    """
    sp_session = scienceParse.session
    try:
        data = sp_session.extract_references(filename)
    except IOError as e:
        raise RuntimeError('ScienceParse extraction failed: %s' % e) from e
    return format_scienceparse_output(data)
