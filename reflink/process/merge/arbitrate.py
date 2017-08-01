"""Generate authoritative reference metadata using validity probabilities."""

from reflink.process.util import argmax
from statistics import mean

from reflink import logging
from itertools import repeat

logger = logging.getLogger(__name__)


def arbitrate(metadata: list, valid: list, priors: list) -> dict:
    """
    Apply arbitration logic to raw extraction metadata for a single reference.

    Parameters
    ----------
    metadata : list
        Each item is a two-tuple of ``(str, dict)``, representing an extractor
        and its metadata record.
    valid : list
        Represents the probability of each field-value in each metadata record
        being valid. Should have the same shape as ``metadata``, except that
        field-values are replaced by ``float`` in the range 0.0 to 1.0.
    priors : list
        Represents prior level of trust in field output for each extractor.

    Returns
    -------
    dict
        Authoritative metadata record for a single extracted reference.
    """


    metadata = dict(metadata)
    valid = dict(valid)
    priors = dict(priors)
    extractors = list(metadata.keys())

    try:
        missing = set(extractors) - set(priors.keys())
        assert len(missing) == 0
    except AssertionError:
        logger.error('Missing priors for %s' % '; '.join(list(missing)))
        raise ValueError('Priors missing for one or more extractors')
    try:
        assert len(metadata) == len(valid)
        assert len(set(metadata.keys()) - set(valid.keys())) == 0
    except AssertionError:
        msg = 'Metadata and validity objects must have the same shape'
        logger.error(msg)
        raise ValueError(msg)

    fields = list({
        field for metadatum in metadata.values() for field in metadatum.keys()
    })

    probs = [
        [
            valid[extractor].get(field, 0.) * priors[extractor].get(field, priors[extractor].get('__all__', 0.))
            for extractor in extractors
        ] for field in fields
    ]

    optimal = [argmax(prob) for prob in probs]
    score = mean([prob[optimal[i]] for i, prob in enumerate(probs)])

    return {
        field: metadata[extractors[optimal[i]]][field]
        for i, field in enumerate(fields)
    }, score


def arbitrate_all(metadata_all: list, valid_all: list,
                  priors_all: list) -> list:
    """
    Helper to apply arbitration to metadata for a set of cited references.

    Parameters
    ----------
    metadata_all : list
        List of lists (see :func:`.arbitrate`).
    valid_all : list
        List of lists (see :func:`.arbitrate`).
    priors_all : list
        List of lists (see :func:`.arbitrate`).

    Returns
    -------
    list
        Optimal metadata for cited references. Each item is a ``dict``. See
        :func:`.arbitrate` for more details.
    """

    return list(map(arbitrate, metadata_all, valid_all,
                    repeat(priors_all, len(metadata_all))))
