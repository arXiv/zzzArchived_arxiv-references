import os
import requests

from reflink import logging
from reflink import types
from reflink.status import HTTP_200_OK

logger = logging.getLogger(__name__)

SCIENCEPARSE_API_VERSION = os.environ.get(
    'REFLINK_SCIENCEPARSE_API_VERSION', 'v1'
)
SCIENCEPARSE_DOCKER_IMAGE = os.environ.get(
    'REFLINK_SCIENCEPARSE_DOCKER_IMAGE',
    'allenai-docker-public-docker.bintray.io/s2/scienceparse:1.2.8-SNAPSHOT'
)
SCIENCEPARSE_DOCKER_PORT = os.environ.get(
    'REFLINK_SCIENCEPARSE_DOCKER_PORT', 8888
)
SCIENCEPARSE_DOCKER_PORT = int(SCIENCEPARSE_DOCKER_PORT)


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


def extract_references(filename: str) -> types.ReferenceMetadata:
    """
    Send the pdf to the ScienceParse service that ought to be running on the
    same machine. If not running, start it up, wait, then send the request.

    Return the reponse formatted to the schema for all references

    Parameters
    ----------
    filename : str
        Name of the pdf from which to extract references

    Returns
    -------
    reference_docs : list of dicts
        Dictionary of reference metadata with metadata separated into author,
        journal, year, etc
    """

    with open(filename, 'rb') as pdfhandle:
        url = 'http://localhost:{}/{}'.format(
            SCIENCEPARSE_DOCKER_PORT, SCIENCEPARSE_API_VERSION
        )
        headers = {'Content-Type': 'application/pdf'}
        response = requests.post(url, data=pdfhandle, headers=headers)

        if response.status_code != HTTP_200_OK:
            msg = 'ScienceParse ({}) return error code {} ({}): {}'.format(
                response.url, response.status_code,
                response.reason, response.content
            )
            logger.error(msg)
            raise RuntimeError(msg)

        data = response.json()

    return format_scienceparse_output(data)
