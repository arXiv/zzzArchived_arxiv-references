"""Service integration for RefExtract."""

import os
from urllib.parse import urljoin
from typing import List

import requests

from references import logging
from references.context import get_application_config, get_application_global

logger = logging.getLogger(__name__)


class RefExtractSession(object):
    """Provides an interface to RefExtract."""

    def __init__(self, endpoint: str) -> None:
        """Set the endpoint for Refextract service."""
        self.endpoint = endpoint
        response = requests.get(urljoin(self.endpoint, '/refextract/status'))
        if not response.ok:
            raise IOError('Refextract endpoint not available: %s' %
                          response.content)

    def extract_references(self, filename: str) -> List[dict]:
        """
        Extract references from the PDF at ``filename``.

        Parameters
        ----------
        filename : str

        Returns
        -------
        list
            Raw output from RefExtract.
        """
        response = requests.post(urljoin(self.endpoint, '/refextract/extract'),
                                 files={'file': open(filename, 'rb')},
                                 timeout=300)
        if not response.ok:
            raise IOError('%s: Refextract extraction failed: %s' %
                          (filename, response.content))
        return response.json()


def init_app(app: object = None) -> None:
    """Set default configuration parameters for an application instance."""
    config = get_application_config(app)
    config.setdefault('REFEXTRACT_ENDPOINT', 'http://localhost:8080')


def get_session(app: object = None) -> RefExtractSession:
    """Get a new refextract session."""
    endpoint = get_application_config(app).get('REFEXTRACT_ENDPOINT')
    if not endpoint:
        raise RuntimeError('Refextract endpoint not set')
    return RefExtractSession(endpoint)


def current_session():
    """Get/create :class:`.RefExtractSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'refextract' not in g:
        g.refextract = get_session()
    return g.refextract


def extract_references(filename: str) -> List[dict]:
    """
    Extract references from the PDF at ``filename``.

    See :meth:`.RefExtractSession.extract_references`.
    """
    return current_session().extract_references(filename)
