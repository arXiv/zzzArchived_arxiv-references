"""
This module encapsulates reference extraction logic.

The processing pipeline is comprised of several independent modules that
perform specific tasks. These modules are stateless, remain totally unaware of
each other, and are not responsible for IO operations (except reading
from/writing to the filesystem). Each module exposes a single method.

Extractor:
- Expects the location of a PDF on the filesystem;
- Extracts reference lines and structured reference metadata;
- Returns reference lines and metadata.
"""

from reflink.process.extract import cermine


def extract_cermine(pdf_path: str) -> list:
    """
    Extract the list of reference metadata from a PDF using CERMINE.

    Parameters
    ----------
    pdf_path : str
        Location of an arXiv PDF on the filesystem.

    Returns
    -------
    list
        Extracted reference metadata.
    """
    return cermine.extract_references(pdf_path)


extract = extract_cermine
