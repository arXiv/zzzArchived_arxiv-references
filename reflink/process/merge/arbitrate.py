"""Generate authoritative reference metadata using validity probabilities."""

from reflink.process.util import argmax
try:
    from reflink import logging
except ImportError:
    import logging

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
        Should have the same shape as ``valid``.

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

    return {
        field: metadata[extractors[argmax([
            valid[extractor].get(field, 0.) * priors[extractor].get(field, 0.)
            for extractor in extractors
        ])]][field] for field in fields
    }


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

    return list(map(arbitrate, metadata_all, valid_all, priors_all))
