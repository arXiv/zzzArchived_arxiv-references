"""Retrieves PDFs of published papers from the core arXiv document store."""

import os
import tempfile
import requests
from references import logging

logger = logging.getLogger(__name__)

ARXIV_HOME = os.environ.get('ARXIV_HOME', 'https://arxiv.org')


def retrievePDF(document_id: str) -> tuple:
    """
    Retrieve PDF for an arXiv document.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    pdf_path : str
    source_path : str
    """
    pdf_response = requests.get('%s/pdf/%s.pdf' % (ARXIV_HOME, document_id))
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
