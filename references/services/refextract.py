"""Service integration for RefExtract."""

import os
from urllib.parse import urljoin
from typing import List
from urllib3 import Retry
import requests

from arxiv.base import logging
from arxiv.base.globals import get_application_config, get_application_global

logger = logging.getLogger(__name__)


class RefExtractSession(object):
    """Provides an interface to RefExtract."""

    def __init__(self, endpoint: str) -> None:
        """Set the endpoint for Refextract service."""
        self.endpoint = endpoint
        self._session = requests.Session()
        self._adapter = requests.adapters.HTTPAdapter(max_retries=2)
        self._session.mount('http://', self._adapter)
        _target = urljoin(self.endpoint, '/refextract/status')
        response = self._session.get(_target)
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
        self._adapter.max_retries = Retry(connect=30, read=10,
                                          backoff_factor=20)
        _target = urljoin(self.endpoint, '/refextract/extract')
        try:
            response = self._session.post(_target,
                                          files={'file': open(filename, 'rb')})
        except requests.exceptions.ConnectionError as e:
            logger.debug('ConnectionError: %s', e)
            raise IOError('%s: Refextract failed: %s' % (filename, e)) from e
        if not response.ok:
            logger.debug('Bad status: %i', response.status_code)
            raise IOError('%s: Refextract failed: %s' %
                          (filename, response.content))
        data: List[dict] = response.json()
        return data


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


def current_session() -> RefExtractSession:
    """Get/create :class:`.RefExtractSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'refextract' not in g:
        g.refextract = get_session()
    session: RefExtractSession = g.refextract
    return session


def extract_references(filename: str) -> List[dict]:
    """
    Extract references from the PDF at ``filename``.

    See :meth:`.RefExtractSession.extract_references`.
    """
    return current_session().extract_references(filename)
