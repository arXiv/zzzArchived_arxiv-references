"""Provides a controller for reference metadata views."""

import logging

from reflink import types
from reflink.services import data_store
from reflink import status

from urllib import parse
from itertools import groupby

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.ERROR)
logger = logging.getLogger(__name__)


class ReferenceMetadataController(object):
    """Controller for reference metadata extracted from arXiv documents."""

    def _get_identifiers(self, reference_data):
        identifiers = {
            'doi': reference_data.get('doi'),
            'title': reference_data.get('title')
        }
        identifiers_raw = reference_data.get('identifiers')
        if identifiers_raw:
            for ident in identifiers_raw:
                identifiers[ident['identifier_type']] = ident['identifier']
        return {k: v for k, v in identifiers.items() if v}

    def resolve(self, document_id: str, reference_id: str)\
            -> types.ControllerResponseData:
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
        reference_data, response_status = self.get(document_id, reference_id)
        print(reference_id)
        print(reference_data)
        if response_status != status.HTTP_200_OK:
            return {
                'explanation': "No data exists for this reference"
            }, response_status


        identifiers = self._get_identifiers(reference_data)

        if 'arxiv' in identifiers:
            url = 'https://arxiv.org/abs/%s' % identifiers['arxiv']
        elif 'doi' in identifiers:
            url = 'https://dx.doi.org/%s' % identifiers['doi']
        elif 'isbn' in identifiers:
            url = 'https://www.worldcat.org/isbn/%s' % identifiers['isbn']
        elif 'title' in identifiers:
            url = 'https://scholar.google.com/scholar?&q=%s' \
                    % parse.quote(identifiers['title'])
        else:
            return {
                'explanation': 'Cannot generate redirect for this reference.'
            }, status.HTTP_404_NOT_FOUND
        return url, status.HTTP_303_SEE_OTHER

    def get(self, document_id: str, reference_id: str)\
            -> types.ControllerResponseData:
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
        data = data_store.get_session()

        try:
            reference = data.retrieve(document_id, reference_id)
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

    def list(self, document_id: str,
             reftype: str='__all__') -> types.ControllerResponseData:
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
        data = data_store.get_session()

        try:
            references = data.retrieve_latest(document_id, reftype=reftype)
        except IOError as e:
            logger.error("Error retrieving data (%s): %s " % (document_id, e))
            return {
                'explanation': "An error occurred while retrieving data"
            }, status.HTTP_500_INTERNAL_SERVER_ERROR

        if references is None:
            logger.info("Request for non-existant record: %s" % document_id)
            return {
                'explanation': "No reference data exists for %s" % document_id
            }, status.HTTP_404_NOT_FOUND

        # Missing values should be null.
        for reference in references:
            for key, value in reference.items():
                if type(value) is list:
                    value = [obj for obj in value if obj]
                if not value:
                    reference[key] = None
                else:
                    reference[key] = value

        return {"references": references}, status.HTTP_200_OK
