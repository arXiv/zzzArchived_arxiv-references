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

from reflink.process.extract import cermine, grobid, refextract
from reflink.process.extract import regex_identifiers, scienceparse
from reflink import logging

logger = logging.getLogger(__name__)

EXTRACTORS = [
    ('cermine', cermine.extract_references),
    ('grobid', grobid.extract_references),
    ('refextract', refextract.extract_references),
    ('scienceparse', scienceparse.extract_references)
]


def extract(pdf_path: str) -> dict:
    """
    Perform reference extractions using all available extractors.

    Parameters
    ----------
    pdf_path : str
        Path to an arXiv PDF on the filesystem.

    Returns
    -------
    dict
        Keys are extractor names, values are lists of reference metadata
        objects (``dict``).
    """

    extractions = {}
    for name, extractor in EXTRACTORS:
        try:
            extractions[name] = extractor(pdf_path)
        except Exception as e:
            logger.info('Extraction failed for %s with %s: %s' %
                        (pdf_path, name, e))
    return extractions
