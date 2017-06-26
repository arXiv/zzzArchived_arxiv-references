from celery import shared_task

from reflink.types import PathTuple

import tempfile


@shared_task
def fake_retrieve(document_id: str) -> PathTuple:
    """
    Emulates retrieving and storing PDF and LaTeX source files for an arXiv
    document.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    pdf_path : str
    source_path : str
    """
    _, pdf_path = tempfile.mkstemp()
    source_path = tempfile.mkdtemp()
    return pdf_path, source_path
