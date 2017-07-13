"""Provides a controller for reference metadata views."""

import logging
from reflink import types

from reflink.services import data_store
from reflink import status

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ReferenceMetadataController(object):
    """Controller for reference metadata extracted from arXiv documents."""

    def get(self, document_id: str, reference_id: str)\
            -> types.ControllerResponseData:
        """
        Get metadata for a specific reference in a document.
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
        return {"references": reference}, status.HTTP_200_OK

    def list(self, document_id: str) -> types.ControllerResponseData:
        """
        Get latest reference metadata for an arXiv document.

        Parameters
        ----------
        document_id : str

        Returns
        -------
        dict
            Response payload.
        int
            HTTP status code.
        """
        data = data_store.get_session()

        try:
            references = data.retrieve_latest(document_id)
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
        return {"references": references}, status.HTTP_200_OK
