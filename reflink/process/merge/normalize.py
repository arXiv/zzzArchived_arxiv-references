"""Normalize and filter final reference extraction."""

from reflink import logging
from statistics import mean, median
from decimal import Decimal
logger = logging.getLogger(__name__)


def filter_records(records: list, threshold: float=0.5) -> tuple:
    """
    Remove low-quality extracted references, and generate a composite score.

    Parameters
    ----------
    records : list
        Items are two-tuples of metadata (``dict``) and record score
        (``float``).
    threshold : float
        Minimum record score to retain.

    Return
    ------
    tuple
        Filtered list of reference metadata (``dict``) and a composite score
        for all retained records (``float``).
    """
    filtered_records = [(dict(list(rec.items()) +
                        [('score', Decimal(sc))]), sc)
                        for rec, sc in records if sc >= threshold]
    if len(filtered_records) == 0:
        return [], 0.
    filtered_records, scores = zip(*filtered_records)
    return list(filtered_records), mean(scores)
