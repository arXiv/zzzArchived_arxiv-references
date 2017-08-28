"""Service layer integration for GROBID."""

import requests
import os
from reflink.status import HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED
from flask import _app_ctx_stack as stack


class GrobidSession(object):
    """Represents a configured session with Grobid."""

    def __init__(self, hostname: str, port: int, path: str) -> None:
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
        self.endpoint = 'http://%s:%s/%s' % (hostname, port, path)
        try:
            head = requests.head(self.endpoint)
        except Exception as e:
            raise IOError('Failed to connect to Grobid at %s: %s' %
                          (self.endpoint, e)) from e

        # Grobid doesn't allow HEAD, but at least a 405 tells us it's running.
        if head.status_code != HTTP_405_METHOD_NOT_ALLOWED:
            raise IOError('Failed to connect to Grobid at %s: %s' %
                          (self.endpoint, head.content))

    def extract_references(self, filename: str):
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
                response = requests.post(self.endpoint, files=files)
        except Exception as e:
            raise IOError('Request to Grobid failed: %s' % e) from e

        if response.status_code != HTTP_200_OK:
            msg = 'GROBID ({}) return error code {} ({}): {}'.format(
                response.url, response.status_code,
                response.reason, response.content
            )
            raise IOError(msg)
        return response.content


class Grobid(object):
    """Grobid integration from reflink worker application."""

    def __init__(self, app=None) -> None:
        """Set and configure an application instance, if provided."""

        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.setdefault('REFLINK_GROBID_HOSTNAME', 'localhost')
        app.config.setdefault('REFLINK_GROBID_PORT', '8889')
        app.config.setdefault('REFLINK_GROBID_PATH', 'processFulltextDocument')

    def get_session(self) -> None:
        """Generate a new configured :class:`.GrobidSession`."""
        try:
            hostname = self.app.config['REFLINK_GROBID_HOSTNAME']
            port = int(self.app.config['REFLINK_GROBID_PORT'])
            path = self.app.config['REFLINK_GROBID_PATH']
        except (RuntimeError, AttributeError) as e:   # No application context.
            hostname = os.environ.get('REFLINK_GROBID_HOSTNAME', 'localhost')
            port = int(os.environ.get('REFLINK_GROBID_PORT', '8889'))
            path = os.environ.get('REFLINK_GROBID_PATH',
                                  'processFulltextDocument')

        return GrobidSession(hostname, port, path)

    @property
    def session(self):
        """Get or create a :class:`.GrobidSession` for the current context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'grobid'):
                ctx.grobid = self.get_session()
            return ctx.grobid
        return self.get_session()     # No application context.


grobid = Grobid()
