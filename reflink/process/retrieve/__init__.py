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


def fake_retrieve(document_id: str) -> tuple:
    """
    A fake implementation of the retrieve task.

    Emulate retrieving PDF and LaTeX source files for an arXiv document.

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


retrieve = fake_retrieve
