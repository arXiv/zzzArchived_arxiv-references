"""Service integration for central arXiv document store."""

import requests
import os
from references import logging
from urllib.parse import urlparse
from flask import _app_ctx_stack as stack
import tempfile

logger = logging.getLogger(__name__)


class RetrievePDFSession(object):
    """Provides an interface to RefExtract."""

    def __init__(self, endpoint: str) -> None:
        """Set the endpoint for Refextract service."""
        self.endpoint = endpoint
        o = urlparse(endpoint)
        response = requests.get('%s://%s' % (o.scheme, o.netloc))
        if not response.ok:
            raise IOError('PDF endpoint not available: %s' %
                          response.content)

    def retrieve(self, document_id: str) -> str:
        """
        Retrieve PDFs of published papers from the core arXiv document store.

        Parameters
        ----------
        document_id : str

        Returns
        -------
        str
            Path to (temporary) PDF.
        """
        target = '%s/pdf/%s.pdf' % (self.endpoint, document_id)
        pdf_response = requests.get(target)
        if pdf_response.status_code == requests.codes.NOT_FOUND:
            logger.info('Could not retrieve PDF for %s' % document_id)
            return None
        elif pdf_response.status_code != requests.codes.ok:
            raise IOError('%s: unexpected status for PDF: %i' %
                          (document_id, pdf_response.status_code))

        _, pdf_path = tempfile.mkstemp(prefix=document_id, suffix='.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        os.chmod(pdf_path, 0o775)
        return pdf_path


class RetrievePDF(object):
    """PDF retrieval from central document store."""

    def __init__(self, app=None):
        """Set and configure application, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        pass

    def get_session(self) -> None:
        """Create a new :class:`.RetrievePDFSession`."""
        try:
            endpoint = self.app.config['PDF_ENDPOINT']
        except (RuntimeError, AttributeError) as e:   # No application context.
            endpoint = os.environ.get('PDF_ENDPOINT')
        return RetrievePDFSession(endpoint)

    @property
    def session(self):
        """Get or create a :class:`.RetrievePDFSession` for this context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'retrieve'):
                ctx.retrieve = self.get_session()
            return ctx.retrieve
        return self.get_session()     # No application context.


retrievePDF = RetrievePDF()
