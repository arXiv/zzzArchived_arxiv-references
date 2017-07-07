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


import tempfile
import requests


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
    if pdf_response.status_code != requests.codes.ok:
        raise IOError('Could not retrieve PDF for %s' % document_id)

    _, pdf_path = tempfile.mkstemp(prefix=document_id, suffix='.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_response.content)

    src_response = requests.get('https://arxiv.org/e-print/%s' % document_id)
    if src_response.status_code != requests.codes.ok:
        raise IOError('Could not retrieve source for %s' % document_id)

    _, source_path = tempfile.mkstemp(prefix=document_id, suffix='.tar.gz')
    with open(source_path, 'wb') as f:
        f.write(src_response.content)

    return pdf_path, source_path
