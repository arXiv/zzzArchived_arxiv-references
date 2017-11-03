"""Service layer integration for GROBID."""

import os
from urllib.parse import urljoin

import requests

from references.status import HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED
from references.context import get_application_config, get_application_global


class GrobidSession(object):
    """Represents a configured session with Grobid."""

    def __init__(self, endpoint: str, path: str) -> None:
        """
        Set up configuration for Grobid, and test the connection.

        Parameters
        ----------
        hostname: str
        port : int
        path : str

        Raises
        ------
        IOError
            Raised when unable to contact Grobid with the provided parameters.
        """
        self.endpoint = endpoint
        self.path = path
        try:
            head = requests.head(urljoin(self.endpoint, self.path))
        except Exception as e:
            raise IOError('Failed to connect to Grobid at %s: %s' %
                          (self.endpoint, e)) from e

        # Grobid doesn't allow HEAD, but at least a 405 tells us it's running.
        if head.status_code != HTTP_405_METHOD_NOT_ALLOWED:
            raise IOError('Failed to connect to Grobid at %s: %s' %
                          (self.endpoint, head.content))

    def extract_references(self, filename: str) -> bytes:
        """
        Extract references from the PDF represented by ``filehandle``.

        Parameters
        ----------
        filename : str

        Returns
        -------
        str
            Raw XML response from Grobid.
        """
        try:
            with open(filename, 'rb') as filehandle:
                files = {'input': filehandle}
                response = requests.post(urljoin(self.endpoint, self.path),
                                         files=files)
        except Exception as e:
            raise IOError('Request to Grobid failed: %s' % e) from e

        if response.status_code != HTTP_200_OK:
            msg = 'GROBID ({}) return error code {} ({}): {}'.format(
                response.url, response.status_code,
                response.reason, response.content
            )
            raise IOError(msg)
        return response.content


def init_app(app: object = None) -> None:
    """Set default configuration parameters for an application instance."""
    config = get_application_config(app)
    config.setdefault('GROBID_ENDPOINT', 'http://localhost:8080')
    config.setdefault('GROBID_PATH', 'processFulltextDocument')


def get_session(app: object = None) -> GrobidSession:
    """Get a new Grobid session."""
    config = get_application_config(app)
    endpoint = config.get('GROBID_ENDPOINT', 'http://localhost:8080')
    path = config.get('GROBID_PATH', 'processFulltextDocument')
    return GrobidSession(endpoint, path)


def current_session():
    """Get/create :class:`.MetricsSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'grobid' not in g:
        g.grobid = get_session()
    return g.grobid


def extract_references(filename: str) -> bytes:
    """
    Extract references from the PDF at ``filename``.

    See :meth:`.GrobidSession.extract_references`.
    """
    return current_session().extract_references(filename)
