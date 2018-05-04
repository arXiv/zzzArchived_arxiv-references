"""Service integration for ScienceParse."""

import requests
import os
from functools import wraps
from typing import List
from urllib.parse import urljoin
from urllib3 import Retry

from arxiv.base.globals import get_application_config, get_application_global
from arxiv.status import HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED

from references.domain import Reference

from .parse import format_scienceparse_output


class ScienceParseSession(object):
    """Represents a connection to the ScienceParse service."""

    def __init__(self, endpoint: str) -> None:
        """
        Set connection parameters and test that ScienceParse is available.

        Parameters
        ----------
        endpoint : str

        Raises
        ------
        IOError
            Raised when unable to connect to ScienceParse with provided
            parameters.
        """
        self.endpoint = endpoint
        self._session = requests.Session()
        self._adapter = requests.adapters.HTTPAdapter(max_retries=2)
        self._session.mount('http://', self._adapter)
        try:
            head = self._session.head(self.endpoint)
        except Exception as e:
            raise IOError('Failed to connect to ScienceParse at %s: %s' %
                          (self.endpoint, e)) from e

        # ScienceParse doesn't allow HEAD, but at least a 405 tells us it's
        #  running.
        if head.status_code != HTTP_405_METHOD_NOT_ALLOWED:
            raise IOError('Failed to connect to ScienceParse at %s: %s' %
                          (self.endpoint, head.content))

        try:
            head = requests.head(self.endpoint)
        except Exception as e:
            msg = 'Failed to connect to ScienceParse at %s: %s' %  \
                    (self.endpoint, e)
            raise IOError(msg) from e

    def extract_references(self, filepath: str) -> List[Reference]:
        """
        Extract references from the PDF represented by ``filehandle``.

        Parameters
        ----------
        filepath : str

        Returns
        -------
        dict
            JSON response from ScienceParse.
        """
        headers = {'Content-Type': 'application/pdf'}

        try:
            with open(filepath, 'rb') as filehandle:
                response = requests.post(self.endpoint, data=filehandle,
                                         headers=headers)
        except Exception as e:
            raise IOError('Request to ScienceParse failed: %s' % e) from e

        if response.status_code != HTTP_200_OK:
            msg = 'ScienceParse ({}) return error code {} ({}): {}'.format(
                response.url, response.status_code,
                response.reason, response.content
            )
            raise IOError(msg)
        data: dict = response.json()
        return format_scienceparse_output(data)


def init_app(app: object = None) -> None:
    """Set default configuration parameters for an application instance."""
    config = get_application_config(app)
    config.setdefault('SCIENCEPARSE_ENDPOINT', 'http://localhost:8080')


def get_session(app: object = None) -> ScienceParseSession:
    """Get a new ScienceParse session."""
    endpoint = get_application_config(app).get('SCIENCEPARSE_ENDPOINT')
    if not endpoint:
        raise RuntimeError('ScienceParse endpoint not set')
    return ScienceParseSession(endpoint)


def current_session() -> ScienceParseSession:
    """Get/create :class:`.ScienceParseSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'scienceparse' not in g:
        g.scienceparse = get_session()
    session: ScienceParseSession = g.scienceparse
    return session


@wraps(ScienceParseSession.extract_references)
def extract_references(filename: str) -> List[Reference]:
    """
    Extract references from the PDF at ``filename``.

    See :meth:`.ScienceParseSession.extract_references`.
    """
    return current_session().extract_references(filename)
