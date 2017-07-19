"""
This module encapsulates PDF and source retrieval logic.

The processing pipeline is comprised of several independent modules that
perform specific tasks. These modules are stateless, remain totally unaware of
each other, and are not responsible for IO operations (except reading
from/writing to the filesystem). Each module exposes a single method.

Retriever:
- Expects an arXiv ID;
- Retrieves the PDF and TeX sources, and stores them on the filesystem;
- Returns the location of the PDF and TeX sources on the filesystem.
"""

import os
import tempfile
import requests
import logging

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
logger = logging.getLogger(__name__)


# EXTENSIONS = {
#     'application/pdf': '.pdf',
#     'application/postscript': '.ps',
#     'application/x-eprint-tar': '.tar.gz',
#     'application/x-eprint': '.tex',
# }




def retrieve(document_id: str) -> tuple:
    """
    Retrieve PDF and LaTeX source files for an arXiv document.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    pdf_path : str
    source_path : str
    """
    pdf_response = requests.get('https://arxiv.org/pdf/%s.pdf' % document_id)
    if pdf_response.status_code == requests.codes.NOT_FOUND:
        logger.info('Could not retrieve PDF for %s' % document_id)
        return None, None
    elif pdf_response.status_code != requests.codes.ok:
        raise IOError('Unexpected status for %s PDF' % document_id)

    _, pdf_path = tempfile.mkstemp(prefix=document_id, suffix='.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_response.content)
    os.chmod(pdf_path, 0o775)

    # Retrieve the document source, if it exists.
    src_response = requests.get('https://arxiv.org/e-print/%s' % document_id)
    content_type = src_response.headers.get('content-type')

    if src_response.status_code == requests.codes.NOT_FOUND:
        logger.info('Could not retrieve source for %s' % document_id)
        source_path = None
    elif content_type != 'application/x-eprint-tar':
        logger.info('No TeX source package available for %s' % document_id)
        source_path = None
    elif src_response.status_code != requests.codes.ok:
        raise IOError('Unexpected status for %s source' % document_id)
    else:
        _, source_path = tempfile.mkstemp(prefix=document_id, suffix='.tar.gz')
        with open(source_path, 'wb') as f:
            f.write(src_response.content)
        os.chmod(source_path, 0o775)

    return pdf_path, source_path
