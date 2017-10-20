"""Provides a controller for reference metadata views."""

import logging

from references.types import ControllerResponseData
from references.services import data_store
from references import status
from flask import url_for
from urllib import parse

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.ERROR)
logger = logging.getLogger(__name__)


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


def resolve(document_id: str, reference_id: str) -> ControllerResponseData:
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
    reference_data, response_status = get(document_id, reference_id)

    if response_status != status.HTTP_200_OK:
        return {
            'explanation': "No data exists for this reference"
        }, response_status

    identifiers = _get_identifiers(reference_data)

    if 'arxiv' in identifiers:
        url = 'https://arxiv.org/abs/%s' % identifiers['arxiv']
    elif 'doi' in identifiers:
        url = 'https://dx.doi.org/%s' % identifiers['doi']
    elif 'isbn' in identifiers:
        url = 'https://www.worldcat.org/isbn/%s' % identifiers['isbn']
    elif 'title' in identifiers:
        url = 'https://scholar.google.com/scholar?%s' % \
              _gs_query(reference_data)
    else:
        return {
            'explanation': 'Cannot generate redirect for this reference.'
        }, status.HTTP_404_NOT_FOUND
    return url, status.HTTP_303_SEE_OTHER


def get(document_id: str, reference_id: str) -> ControllerResponseData:
    """
    Get metadata for a specific reference in a document.

    Parameters
    ----------
    document_id : str
    reference_id : str

    Returns
    -------
    dict
    int
        HTTP status code.
    """
    try:
        reference = data_store.get_reference(document_id, reference_id)
    except IOError as e:
        logger.error("Error retrieving reference (%s, %s): %s "
                     % (document_id, reference_id, e))
        return {
            'explanation': "An error occurred while retrieving data"
        }, status.HTTP_500_INTERNAL_SERVER_ERROR

    if reference is None:
        logger.info("Request for non-existant reference: %s, %s"
                    % (document_id, reference_id))
        return {
            'explanation': "No reference data exists for %s" % document_id
        }, status.HTTP_404_NOT_FOUND

    for key, value in reference.items():
        if type(value) is list:
            value = [obj for obj in value if obj]
        if not value:
            reference[key] = None
        else:
            reference[key] = value
    return reference, status.HTTP_200_OK


def list(document_id: str, reftype: str='__all__') -> ControllerResponseData:
    """
    Get latest reference metadata for an arXiv document.

    Parameters
    ----------
    document_id : str
    reftype : str

    Returns
    -------
    dict
        Response payload.
    int
        HTTP status code.
    """
    try:
        data = data_store.get_latest_extractions(document_id, reftype=reftype)
    except IOError as e:
        logger.error("Error retrieving data (%s): %s " % (document_id, e))
        return {
            'explanation': "An error occurred while retrieving data"
        }, status.HTTP_500_INTERNAL_SERVER_ERROR

    if data is None:
        logger.info("Request for non-existant record: %s" % document_id)
        return {
            'explanation': "No reference data exists for %s" % document_id
        }, status.HTTP_404_NOT_FOUND

    # Missing values should be null.
    for reference in data['references']:
        for key, value in reference.items():
            if type(value) is list:
                value = [obj for obj in value if obj]
            if not value:
                reference[key] = None
            else:
                reference[key] = value
    if 'extractors' in data:
        data['extractors'] = {
            extractor: url_for('references.raw', doc_id=document_id,
                               extractor=extractor)
            for extractor in data['extractors']
        }
    return data, status.HTTP_200_OK


def get_raw_extraction(document_id: str,
                       extractor: str) -> ControllerResponseData:
    """
    Get raw extraction metadata for a specific extractor.

    Parameters
    ----------
    document_id : str
    extractor : str

    Returns
    -------
    dict
        Response payload.
    int
        HTTP status code.
    """
    try:
        extraction = data_store.get_raw_extraction(document_id, extractor)
    except IOError as e:
        logger.error("Error retrieving data (%s): %s " % (document_id, e))
        return {
            'explanation': "An error occurred while retrieving data"
        }, status.HTTP_500_INTERNAL_SERVER_ERROR

    if extraction is None:
        logger.info("Request for non-existant record: %s, %s" %
                    (document_id, extractor))
        return {
            'explanation': "No reference data exists for %s from %s" %
            (document_id, extractor)
        }, status.HTTP_404_NOT_FOUND
    return extraction, status.HTTP_200_OK
