"""
Encapsulates the reference extraction process.

Leverages third-party reference extraction services to generate a set of raw
bibliographic metadata.
"""

from typing import Dict, List, Callable, Tuple
from datetime import datetime
from statistics import mean

from arxiv.base import logging

from references.services import cermine, grobid, refextract, scienceparse
from references.domain import Reference

logger = logging.getLogger(__name__)

EXTRACTORS: List[Tuple[str, Callable]] = [
    ('cermine', cermine.extract_references),
    ('grobid', grobid.extract_references),
    ('refextract', refextract.extract_references),
    ('scienceparse', scienceparse.extract_references),
]


def getDefaultExtractors() -> List[Tuple[str, Callable]]:
    """Get the default extractors for this service."""
    return EXTRACTORS


def estimate_quality(metadata: list) -> float:
    """
    Generate a rough estimate of extraction quality.

    Parameters
    ----------
    metadata : list
        A list of metadata (dict) from a single extractor.

    Returns
    -------
    float
    """
    return mean([
        len([(key, value) for key, value in metadatum.items() if value])/10.
        for metadatum in metadata
    ])


def extract(pdf_path: str, document_id: str,
            extractors: list = getDefaultExtractors()) \
        -> Dict[str, List[Reference]]:
    """
    Perform reference extractions using all available extractors.

    Parameters
    ----------
    pdf_path : str
        Path to an arXiv PDF on the filesystem.
    document_id : str
        Identifier for an arXiv paper.
    extractors : list
        Tuples of ('extractor name', callable).

    Returns
    -------
    dict
        Keys are extractor names, values are lists of reference metadata
        objects (``dict``).
    """
    extractions = {}
    for name, extractor in extractors:
        logger.debug('%s: starting extraction with %s', document_id, name)
        try:
            extractions[name] = extractor(pdf_path, document_id)
            logger.debug('%s: extraction with %s succeeded', document_id, name)
        except Exception as e:
            logger.debug('%s: extraction failed for %s with %s: %s',
                         document_id, pdf_path, name, e)
    return extractions
