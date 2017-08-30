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

from reflink.process.extract import cermine, grobid, refextract, scienceparse
from reflink import logging
from reflink.services.metrics import metrics
from datetime import datetime
from statistics import mean


logger = logging.getLogger(__name__)

EXTRACTORS = [
    ('cermine', cermine.extract_references),
    ('grobid', grobid.extract_references),
    ('refextract', refextract.extract_references),
    # ('scienceparse', scienceparse.extract_references)
]


def getDefaultExtractors():
    return EXTRACTORS


def estimate_quality(metadata: list) -> float:
    """"
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
        for metadatum in metadata])


def extract(pdf_path: str, document_id: str,
            extractors: list=getDefaultExtractors(), report=True) -> dict:
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
        logger.info('Starting extraction with %s' % name)
        try:
            start_time = datetime.now()
            extractions[name] = extractor(pdf_path, document_id)
            end_time = datetime.now()

            logger.info('Extraction with %s succeeded' % name)
            if report:
                metrics.session.report('ExtractionSucceeded', 1.,
                                       dimensions={'Extractor': name})
                metrics.session.report('ExtractionDuration',
                                       (start_time - end_time).microseconds,
                                       dimensions={'Extractor': name},
                                       units='Microseconds')
                metrics.session.report('ExtractionQuality',
                                       estimate_quality(extractions[name]),
                                       dimensions={'Extractor': name},
                                       units='Microseconds')

        except Exception as e:
            if report:
                metrics.session.report('ExtractionSucceeded', 0.,
                                       dimensions={'Extractor': name})
            logger.info('Extraction failed for %s with %s: %s' %
                        (pdf_path, name, e))
    return extractions
