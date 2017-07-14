"""
This module encapsulates reference injection logic.

The processing pipeline is comprised of several independent modules that
perform specific tasks. These modules are stateless, remain totally unaware of
each other, and are not responsible for IO operations (except reading
from/writing to the filesystem). Each module exposes a single method.

Link injector:
- Expects the location of a TeX source bundle on the filesystem, and structured
  metadata returned by the extractor;
- Injects reference links into the TeX source, and calls the TeX processor to
  generate a PDF;
- Returns the location of the new link-injected PDF on the filesystem.
"""
import tempfile
from reflink.process.inject.latexinjector import inject_urls


def fake_inject(source_path: str, metadata: list) -> str:
    """
    Fake implementation of the inject task.

    Emulate injecting links into a PDF using reference metadata.

    Parameters
    ----------
    source_path : str
        The location of an arXiv TeX source bundle on the filesystem.
    metadata : list
        A list of reference metadata records (dict) extracted from the arXiv
        document.

    Returns
    -------
    str
        Path to link-injected PDF.
    """
    _, pdf_path = tempfile.mkstemp()
    return pdf_path


inject = inject_urls
