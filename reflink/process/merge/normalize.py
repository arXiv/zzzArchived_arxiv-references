"""Normalize and filter final reference extraction."""

from statistics import mean


def filter_records(records: list, threshold: float=0.6) -> tuple:
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

    filtered_records, scores = zip(*[
        (rec, sc) for rec, sc in records if sc >= threshold
    ])
    return list(filtered_records), mean(scores)
