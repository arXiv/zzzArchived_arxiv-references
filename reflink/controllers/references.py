import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

from reflink import types

from reflink.services import data_store
from reflink import status


class ReferenceMetadataController(object):
    """
    Controller for retrieving reference metadata extracted from arXiv documents.
    """
    def get(self, document_id: str) -> types.ControllerResponseData:
        """
        Get reference metadata for an arXiv document.

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
            references = data.retrieve(document_id)
        except IOError as e:
            logger.error("Error retrieving data for %s: %s " % (document_id, e))
            return {
                'explanation': "An error occurred while retrieving data"
            }, status.HTTP_500_INTERNAL_SERVER_ERROR

        if references is None:
            logger.info("Request for non-existant record: %s" % document_id)
            return {
                'explanation': "No reference data exists for %s" % document_id
            }, status.HTTP_404_NOT_FOUND
        return {"references": references}, status.HTTP_200_OK
