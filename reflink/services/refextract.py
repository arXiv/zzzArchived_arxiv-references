"""Service integration for RefExtract."""

from refextract import extract_references_from_file

from reflink import logging
from flask import _app_ctx_stack as stack
logging.getLogger('refextract.references.engine').setLevel(40)
logger = logging.getLogger(__name__)


class RefExtractSession(object):
    """Provides an interface to RefExtract."""

    def extract_references(self, filename):
        """
        Extract references from the PDF at ``filename``.

        Parameters
        ----------
        filename : str

        Returns
        -------
        dict
            Raw output from RefExtract.
        """
        try:
            data = extract_references_from_file(filename)
        except Exception as e:
            msg = 'Request to RefExtract failed: %s' % e
            logger.error(msg)
            raise IOError(msg) from e
        return data


class RefExtract(object):
    """RefExtract integration from reflink worker application."""

    def __init__(self, app=None):
        """Set and configure application, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        pass

    def get_session(self) -> None:
        """Create a new :class:`.RefExtractSession`."""
        return RefExtractSession()

    @property
    def session(self):
        """Get or create a :class:`.ScienceParseSession` for this context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'refextract'):
                ctx.refextract = self.get_session()
            return ctx.refextract
        return self.get_session()     # No application context.
