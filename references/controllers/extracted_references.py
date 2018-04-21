"""Provides a controller for reference metadata views."""

from typing import Tuple
from urllib import parse

from flask import url_for
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest

from arxiv import status
from arxiv.base import logging
from references.services import data_store
from references.domain import ReferenceSet

logger = logging.getLogger(__name__)

ControllerResponse = Tuple[dict, int, dict]


# TODO: this might need some work.
def _gs_query(reference: dict) -> str:
    """Generate a query for Google Scholar."""
    query_parts = []
    title = reference.get('title')
    year = reference.get('year')
    authors = reference.get('authors')
    source = reference.get('source')
    if title:
        query_parts.append(('as_q', title))
    if authors:
        for author in authors:
            forename = author.get('forename')
            surname = author.get('surname')
            fullname = author.get('fullname')
            if surname:
                if forename:
                    query_parts.append(('as_sauthors',
                                        '"%s %s"' % (forename, surname)))
                else:
                    query_parts.append(('as_sauthors', '%s' % surname))
            elif fullname:
                query_parts.append(('as_sauthors', '%s' % fullname))
    if source:
        query_parts.append(('as_publication', source))
    if year:
        query_parts.append(('as_ylo', str(year)))
        query_parts.append(('as_yhi', str(year)))
    return parse.urlencode(query_parts)


def _get_identifiers(reference_data: dict) -> dict:
    identifiers = {
        'doi': reference_data.get('doi'),
        'title': reference_data.get('title')
    }
    identifiers_raw = reference_data.get('identifiers')
    if identifiers_raw:
        for ident in identifiers_raw:
            identifiers[ident['identifier_type']] = ident['identifier']
    return {k: v for k, v in identifiers.items() if v}


def resolve(document_id: str, reference_id: str) -> ControllerResponse:
    """
    Get a redirect URL for a reference.

    Parameters
    ----------
    document_id : str
    reference_id : str

    Returns
    -------
    str
    int
    """
    reference_data, response_status, _ = get(document_id, reference_id)
    if response_status != status.HTTP_200_OK:
        content = {
            'explanation': "No data exists for this reference"
        }
    else:
        response_status = status.HTTP_303_SEE_OTHER
        identifiers = _get_identifiers(reference_data)
        if 'arxiv' in identifiers:
            content = {'url': 'https://arxiv.org/abs/%s' %
                              identifiers['arxiv']}
        elif 'doi' in identifiers:
            content = {'url': 'https://dx.doi.org/%s' % identifiers['doi']}
        elif 'isbn' in identifiers:
            content = {'url': 'https://www.worldcat.org/isbn/%s' %
                              identifiers['isbn']}
        elif 'title' in identifiers:
            content = {'url': 'https://scholar.google.com/scholar?%s' %
                              _gs_query(reference_data)}
        else:
            content = {'explanation': 'cannot provide redirect for reference'}
            response_status = status.HTTP_404_NOT_FOUND
    return content, response_status, {}


def get(document_id: str, ref_id: str) -> ControllerResponse:
    """
    Get metadata for a specific reference in a document.

    Parameters
    ----------
    document_id : str
    ref_id : str
        Unique identifer for the reference. This was originally calculated from
        the reference content.

    Returns
    -------
    dict
        Response content.
    int
        HTTP status code.
    dict
        Response headers.
    """
    try:
        rset: ReferenceSet = data_store.load(document_id)
    except data_store.CommunicationError as e:
        logger.error("Couldn't connect to data store")
        raise InternalServerError({'reason': "Couldn't connect to data store"})
    except data_store.ReferencesNotFound as e:
        logger.error("Couldn't connect to data store")
        raise NotFound({'reason': "No such reference"})

    try:
        reference = [r for r in rset.references if r.identifier == ref_id][0]
    except IndexError:
        logger.error("No such reference: %s", ref_id)
        raise NotFound({'reason': 'No such reference'})
    return reference.to_dict(), status.HTTP_200_OK, {}


def list(document_id: str, extractor: str = 'combined') -> ControllerResponse:
    """
    Get latest reference metadata for an arXiv document.

    Parameters
    ----------
    document_id : str
    reftype : str

    Returns
    -------
    dict
        Response content.
    int
        HTTP status code.
    dict
        Response headers.
    """
    try:
        reference_set = data_store.load(document_id, extractor=extractor)
    except data_store.CommunicationError as e:
        raise InternalServerError({'reason': 'Could not retrieve references'})
    except data_store.ReferencesNotFound as e:
        raise NotFound({'reason': 'No such references found'})
    return reference_set.to_dict(), status.HTTP_200_OK, {}
