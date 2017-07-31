"""Provides a controller for PDF views."""

import logging
from flask import current_app
from reflink import types
from reflink.services import ObjectStore
from reflink import status

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(__name__)


class PDFController(object):
    """Controller for retrieving link-injected PDFs from arXiv documents."""

    def __init__(self):
        try:
            self.object_store = ObjectStore(current_app)
        except RuntimeError:    # No application context.
            self.object_store = ObjectStore()

    def get(self, document_id: str) -> types.ControllerResponseData:
        """
        Get the URL of a link-injected PDF for an arXiv publication.

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
        try:
            session = self.object_store.session
        except IOError as e:
            return {
                'explanation': 'Could not access the object store.'
            }, status.HTTP_500_INTERNAL_SERVER_ERROR

        try:
            url = session.retrieve_url(document_id)
        except IOError as e:
            logger.error("Error when retrieving PDF info for document:"
                         " %s: %s" % (document_id, e))
            return {
                'explanation': "An error occurred while retrieving PDF data."
            }, status.HTTP_500_INTERNAL_SERVER_ERROR

        if url is None:
            logger.info("Request for non-existant record: %s" % document_id)
            return {
                'explanation': "No enhanced PDF exists for %s" % document_id
            }, status.HTTP_404_NOT_FOUND
        return {"url": url}, status.HTTP_200_OK
