"""Service integration for central arXiv document store."""

import os
import time
from datetime import datetime, timedelta
import json
from urllib.parse import urljoin

import requests
from flask import _app_ctx_stack as stack

from references import logging
from references.context import get_application_config, get_application_global

logger = logging.getLogger(__name__)


class RequestExtractionSession(object):
    """Provides an interface to the reference extraction service."""

    def __init__(self, endpoint: str) -> None:
        """Set the endpoint for Refextract service."""
        self.endpoint = endpoint
        response = requests.get(urljoin(self.endpoint, '/status'))
        if not response.ok:
            raise IOError('Extraction endpoint not available: %s',
                          response.content)

    def extract(self, document_id: str, pdf_url: str) -> dict:
        """
        Request reference extraction.

        Parameters
        ----------
        document_id : str
        pdf_url : str

        Returns
        -------
        dict
        """
        payload = {'document_id': document_id, 'url': pdf_url}
        response = requests.post(urljoin(self.endpoint, '/references'),
                                 json=payload)
        if not response.ok:
            raise IOError('Extraction request failed with status %i: %s' %
                          (response.status_code, response.content))

        target_url = urljoin(self.endpoint, '/references/%s' % document_id)

        failed = 0
        start = datetime.now()    # If this runs too long, we'll abort.
        while not response.url.startswith(target_url):
            if failed > 2:    # TODO: make this configurable?
                logger.error('%s: cannot get extraction state: %s, %s',
                             document_id, response.status_code,
                             response.content)
                raise IOError("Failed to get extraction state")

            if datetime.now() - start > timedelta(seconds=300):
                logger.error('%s: extraction running after five minutes',
                             document_id)
                raise IOError('Extraction exceeded five minutes')

            time.sleep(2 + failed * 2)    # Back off.
            try:
                response = requests.get(response.url)
            except Exception as e:
                logger.error('%s: cannot get extraction state: %s',
                             document_id, e)
                raise IOError("Failed to get extraction state") from e

            if not response.ok:
                failed += 1

        return response.json()


def get_session(app: object = None) -> RequestExtractionSession:
    """Get a new extraction session."""
    endpoint = get_application_config(app).get('EXTRACTION_ENDPOINT')
    if not endpoint:
        raise RuntimeError('No extraction endpoint set')
    return RequestExtractionSession(endpoint)


def current_session():
    """Get/create :class:`.MetricsSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'extract' not in g:
        g.extract = get_session()
    return g.extract
