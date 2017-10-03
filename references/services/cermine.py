"""Service layer integration for CERMINE."""

import os
import requests
# See http://flask.pocoo.org/docs/0.12/extensiondev/
from flask import _app_ctx_stack as stack
from urllib.parse import urljoin


class ExtractionError(Exception):
    """Encountered an unexpected state during extraction."""

    pass


class CermineSession(object):
    """Represents a configured Cermine session."""

    def __init__(self, endpoint: str) -> None:
        """
        Set the Cermine endpoint.

        Parameters
        ----------
        endpoint : str
        """
        self.endpoint = endpoint
        response = requests.get(urljoin(self.endpoint, '/cermine/status'))
        if not response.ok:
            raise IOError('CERMINE endpoint not available: %s' %
                          response.content)

    def extract_references(self, filename: str, cleanup: bool=False):
        """
        Extract references from the PDF represented by ``filehandle``.

        Parameters
        ----------
        filename : str

        Returns
        -------
        str
            Raw XML response from Cermine.
        """
        # This can take a while.
        response = requests.post(urljoin(self.endpoint, '/cermine/extract'),
                                 files={'file': open(filename, 'rb')},
                                 timeout=300)
        if not response.ok:
            raise IOError('%s: CERMINE extraction failed: %s' %
                          (filename, response.content))
        return response.content


class Cermine(object):
    """Cermine integration from references worker application."""

    def __init__(self, app=None):
        """Set and configure the current application instance, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.setdefault('REFLINK_CERMINE_DOCKER_IMAGE', 'arxiv/cermine')

    def get_session(self) -> CermineSession:
        """Generate a new configured :class:`.CermineSession`."""
        try:
            endpoint = self.app.config['CERMINE_ENDPOINT']
            # image = self.app.config['REFLINK_CERMINE_DOCKER_IMAGE']
        except (RuntimeError, AttributeError) as e:   # No application context.
            endpoint = os.environ.get('CERMINE_ENDPOINT')
            # image = os.environ.get('REFLINK_CERMINE_DOCKER_IMAGE',
            #                        'arxiv/cermine')
        return CermineSession(endpoint)

    @property
    def session(self):
        """Get or creates a :class:`.CermineSession` for the current app."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'cermine'):
                ctx.cermine = self.get_session()
            return ctx.cermine
        return self.get_session()     # No application context.


cermine = Cermine()
