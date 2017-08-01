from reflink import logging
from reflink.process.merge import align, arbitrate, priors, beliefs, normalize
from statistics import mean

logger = logging.getLogger(__name__)


def merge_records(records: dict,
                  extractor_priors: list=priors.EXTRACTORS) -> list:
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
        E.g. ``{"cermine": [references], "scienceparse": [references]}``.
    extractor_priors : list
        Represents prior level of trust in field output for each extractor.

    Returns
    -------
    list
        Authoritative reference metadata. Each item represents a single
        cite reference (``dict``).
    """

    try:
        aligned_records = align.align_records(records)
    except Exception as e:
        raise RuntimeError('Alignment failed: %s' % e) from e
    logger.info(str(aligned_records))

    try:
        aligned_probabilities = beliefs.validate(aligned_records)
    except Exception as e:
        raise RuntimeError('Validation failed: %s' % e) from e

    try:
        arbitrated_records = arbitrate.arbitrate_all(aligned_records,
                                                     aligned_probabilities,
                                                     extractor_priors)
    except Exception as e:
        raise RuntimeError('Arbitration failed: %s' % e) from e

    try:
        final_records = normalize.filter_records(arbitrated_records)
    except Exception as e:
        raise RuntimeError('Filtering failed: %s' % e) from e
    return final_records
