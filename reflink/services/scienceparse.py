"""Service integration for ScienceParse."""

import requests
import os
from reflink.status import HTTP_200_OK
from flask import _app_ctx_stack as stack


class ScienceParseSession(object):
    """Represents a connection to the ScienceParse service."""

    def __init__(self, hostname: str, port: int, path: str) -> None:
        """
        Set connection parameters and test that ScienceParse is available.

        Parameters
        ----------
        hostname : str
        port : int
        path : str

        Raises
        ------
        IOError
            Raised when unable to connect to ScienceParse with provided
            parameters.
        """
        self.endpoint = 'http://%s:%i/%s' % (hostname, port, path)
        try:
            head = requests.head(self.endpoint)
        except Exception as e:
            msg = 'Failed to connect to ScienceParse at %s: %s' %  \
                    (self.endpoint, e)
            raise IOError(msg) from e

        if head.status_code != HTTP_200_OK:
            msg = 'Failed to connect to ScienceParse at %s: %s' %  \
                    (self.endpoint, head.content)
            raise IOError(msg)

    def extract_references(self, filepath: str) -> dict:
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

        return response.json()


class ScienceParse(object):
    """ScienceParse integration from reflink worker application."""

    def __init__(self, app=None) -> None:
        """Set and configure application, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.setdefault('REFLINK_SCIENCEPARSE_HOSTNAME', 'localhost')
        app.config.setdefault('REFLINK_SCIENCEPARSE_PORT', '8888')
        app.config.setdefault('REFLINK_SCIENCEPARSE_PATH', 'v1')

    def get_session(self) -> None:
        """Create a new :class:`.ScienceParseSession`."""
        try:
            hostname = self.app.config['REFLINK_SCIENCEPARSE_HOSTNAME']
            port = int(self.app.config['REFLINK_SCIENCEPARSE_PORT'])
            path = self.app.config['REFLINK_SCIENCEPARSE_PATH']
        except (RuntimeError, AttributeError) as e:   # No application context.
            hostname = os.environ.get('REFLINK_SCIENCEPARSE_HOSTNAME',
                                      'localhost')
            port = int(os.environ.get('REFLINK_SCIENCEPARSE_PORT', '8888'))
            path = os.environ.get('REFLINK_SCIENCEPARSE_PATH', 'v1')
        return ScienceParseSession(hostname, port, path)

    @property
    def session(self):
        """Get or create a :class:`.ScienceParseSession` for this context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'scienceparse'):
                ctx.scienceparse = self.get_session()
            return ctx.scienceparse
        return self.get_session()     # No application context.
