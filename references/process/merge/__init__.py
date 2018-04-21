"""
This module is responsible for merging extracted reference metadata.

The :mod:`references.process.extract` module provides several reference
extraction mechanisms, each of which provides variable levels of completeness
and quality.



.. automodule:: merge
   :members:

"""
from typing import Tuple, List, Dict

from arxiv.base import logging
from references.domain import Reference
from references.process.merge import align, arbitrate, priors, beliefs
from references.process.merge.priors import EXTRACTORS
from references.process.merge import normalize

logger = logging.getLogger(__name__)


def merge_records(records: Dict[str, List[Reference]],
                  extractor_priors: list = EXTRACTORS) \
        -> Tuple[List[Reference], float]:
    """
    Merge extracted references into a single authoritative set of references.

    Takes a list of reference metadata records (each formatted according to the
    schema) and reconciles them with each other to form one primary record for
    each item. First step is to match the lists against each other using
    similarity measures. Then, for each individual record we combine the
    possible fields and augment them with possible external information to form
    a single record.

    Parameters
    ----------
    records : dict
        The reference records from multiple extraction servies/lookup services.
        Keys are extractor names, values are lists of references (dict).
        E.g. ``{"cermine": [references], "grobid": [references]}``.
    extractor_priors : list
        Represents prior level of trust in field output for each extractor.

    Returns
    -------
    list
        Authoritative reference metadata. Each item represents a single
        cite reference (``dict``).
    """
    N_extractions = len(records)
    records = {extractor: normalize.normalize_records(extraction)
               for extractor, extraction in records.items()}
    try:
        aligned_records = align.align_records(records)
    except Exception as e:
        raise RuntimeError('Alignment failed: %s' % e) from e

    try:
        aligned_probabilities = beliefs.validate(aligned_records)
    except Exception as e:
        raise RuntimeError('Validation failed: %s' % e) from e

    try:
        arbitrated_records = arbitrate.arbitrate_all(aligned_records,
                                                     aligned_probabilities,
                                                     extractor_priors,
                                                     N_extractions)
    except Exception as e:
        raise RuntimeError('Arbitration failed: %s' % e) from e

    try:
        final_records = normalize.filter_records(arbitrated_records)
    except Exception as e:
        raise RuntimeError('Filtering failed: %s' % e) from e
    return final_records
