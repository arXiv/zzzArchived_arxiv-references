"""Validate extracted reference metadata."""

from reflink.process.merge.priors import FIELDS


def validate(aligned_references: list) -> list:
    """
    Apply expectations about field values to generate probabilities.

    Parameters
    ----------
    aligned_references : list
        Each item represents an aligned reference; should be a two-tuple with
        the extractor name (``str``) and the reference object (``dict``).

    Returns
    -------
    list
        Probabilities for each value in each field in each reference. Should
        have the same shape as ``aligned_references``, except that values are
        replaced by floats in the range 0.0 to 1.0.
    """

    return [
        [
            (extractor, {
                field: FIELDS.get(field, lambda value: 0.5)(value)
                for field, value in metadatum.items()
            })
            for extractor, metadatum in reference
        ] for reference in aligned_references
    ]
