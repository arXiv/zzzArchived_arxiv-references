"""
This module encapsulates the reference extraction process.


"""

from datetime import datetime
from statistics import mean

from references.process.extract import cermine, grobid, refextract
from references import logging
from references.services import metrics

logger = logging.getLogger(__name__)

EXTRACTORS = [
    ('cermine', cermine.extract_references),
    ('grobid', grobid.extract_references),
    ('refextract', refextract.extract_references),
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
        logger.debug('%s: starting extraction with %s', document_id, name)
        try:
            start_time = datetime.now()
            extractions[name] = extractor(pdf_path, document_id)
            end_time = datetime.now()

            logger.debug('%s: extraction with %s succeeded', document_id, name)
            if report:
                metrics.report('ExtractionSucceeded', 1.,
                               dimensions={'Extractor': name})
                metrics.report('ExtractionDuration',
                               (start_time - end_time).microseconds,
                               dimensions={'Extractor': name},
                               units='Microseconds')
                metrics.report('ExtractionQuality',
                               estimate_quality(extractions[name]),
                               dimensions={'Extractor': name},
                               units='Microseconds')

        except Exception as e:
            if report:
                metrics.report('ExtractionSucceeded', 0.,
                               dimensions={'Extractor': name})
            logger.debug('%s: extraction failed for %s with %s: %s',
                         document_id, pdf_path, name, e)
    return extractions
