"""Service integration for central arXiv document store."""

import os
from urllib.parse import urlparse
from functools import wraps
import tempfile

import requests
from flask import _app_ctx_stack as stack

from arxiv.base import logging
from arxiv.base.globals import get_application_config, get_application_global

logger = logging.getLogger(__name__)


class PDFNotFound(RuntimeError):
    """PDF could not be found."""


class RetrieveFailed(IOError):
    """Failed to retrieve PDF."""


class InvalidURL(ValueError):
    """An invalid URL was requested."""


class RetrievePDFSession(object):
    """Provides an interface to get PDF."""

    def __init__(self, whitelist: list) -> None:
        """Set the endpoint for Refextract service."""
        self._whitelist = whitelist

    def is_valid_url(self, url: str) -> bool:
        """
        Evaluate whether or not a URL is acceptible for retrieval.

        Parameters
        ----------
        url : str
            Location of a document.

        Returns
        -------
        bool
        """
        o = urlparse(url)
        if o.netloc not in self._whitelist:
            return False
        return True

    def retrieve(self, target: str, document_id: str) -> str:
        """
        Retrieve PDFs of published papers from the core arXiv document store.

        Parameters
        ----------
        target : str
        document_id : str

        Returns
        -------
        str
            Path to (temporary) PDF.

        Raises
        ------
        ValueError
            If a disallowed or otherwise invalid URL is passed.
        IOError
            When there is a problem retrieving the resource at ``target``.
        """
        if not self.is_valid_url(target):
            logger.error('Target URL not valid: %s', target)
            raise InvalidURL('URL not allowed: %s' % target)

        pdf_response = requests.get(target)
        status_code = pdf_response.status_code
        if status_code == requests.codes.NOT_FOUND:
            logger.error('Could not retrieve PDF for %s', document_id)
            raise PDFNotFound('Could not retrieve PDF')
        elif status_code != requests.codes.ok:
            logger.error('Failed to retrieve PDF %s: %s, %s',
                         document_id, status_code, pdf_response.content)
            raise RetrieveFailed('Unexpected status: %i' % status_code)

        _, pdf_path = tempfile.mkstemp(prefix=document_id.split('/')[-1],
                                       suffix='.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        os.chmod(pdf_path, 0o775)
        return pdf_path


def init_app(app: object = None) -> None:
    """Set default configuration parameters for an application instance."""
    config = get_application_config(app)
    config.setdefault('SOURCE_WHITELIST', 'arxiv.org,export.arxiv.org')


def get_session(app: object = None) -> RetrievePDFSession:
    """Generate a new :class:`.RetrievePDFSession` session."""
    config = get_application_config(app)
    whitelist = config.get('SOURCE_WHITELIST', 'arxiv.org,export.arxiv.org')
    return RetrievePDFSession(whitelist.split(','))


def current_session() -> RetrievePDFSession:
    """Get/create :class:`.RetrievePDFSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'retrieve' not in g:
        g.retrieve = get_session()
    session: RetrievePDFSession = g.retrieve
    return session


@wraps(RetrievePDFSession.is_valid_url)
def is_valid_url(url: str) -> bool:
    """Evaluate whether or not a URL is acceptible for retrieval."""
    return current_session().is_valid_url(url)


@wraps(RetrievePDFSession.retrieve)
def retrieve_pdf(target: str, document_id: str) -> str:
    """Retrieve PDF of a published paper from the core arXiv document store."""
    return current_session().retrieve(target, document_id)
