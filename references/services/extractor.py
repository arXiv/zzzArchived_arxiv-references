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
        self._session = requests.Session()
        self._adapter = requests.adapters.HTTPAdapter(max_retries=2)
        self._session.mount('http://', self._adapter)

    def status(self):
        try:
            response = self._session.get(urljoin(self.endpoint, '/status'))
        except IOError:
            return False
        if not response.ok:
            return False
        return True

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
        response = self._session.post(urljoin(self.endpoint, '/references'),
                                      json=payload)
        if not response.ok:
            raise IOError('Extraction request failed with status %i: %s' %
                          (response.status_code, response.content))

        target_url = urljoin(self.endpoint, '/references/%s' % document_id)
        try:
            status_url = response.headers['Location']
        except KeyError:
            status_url = response.url
        if status_url == target_url:    # Extraction already performed.
            return response.json()

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
                response = requests.get(status_url)
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
        raise RuntimeError('EXTRACTION_ENDPOINT not set')
    return RequestExtractionSession(endpoint)


def current_session():
    """Get/create :class:`.MetricsSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'extract' not in g:
        g.extract = get_session()
    return g.extract
