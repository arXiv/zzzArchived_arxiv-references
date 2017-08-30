"""Generate authoritative reference metadata using validity probabilities."""

from reflink.process.util import argmax
from statistics import mean
from collections import Counter, defaultdict
from reflink import logging
from reflink.process.merge.align import align_records
from itertools import repeat

import editdistance    # For string similarity.


logger = logging.getLogger(__name__)


def _dict_repr(value: dict) -> str:
    """
    Represent a ``dict`` as a string-coerced list of ``(key, value)`` tuples.

    Parameters
    ----------
    value : dict

    Returns
    -------
    str
    """
    def _keysort(item):
        return item[0]

    return str([(key.strip(), value.strip()) for key, value
                in sorted(value.items(), key=_keysort)])


def _validate(extractors: list, priors: dict, metadata: dict,
              valid: dict) -> None:
    """Check that extraction data is valid for arbitrartion."""
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


def _similarity(value_a: object, value_b: object) -> float:
    """
    Calculate the similarity of two field values.

    Parameters
    ----------
    value_a : object
    value_b : object

    Returns
    -------
    float
        Similarity value in (0. - 1.).
    """
    # Since we need a value in 0-1., diff for numbers is 1. - % diff.
    if type(value_a) in [int, float] and type(value_b) in [int, float]:
        diff = value_a - value_b
        if mean([value_a, value_b]) == 0:
            return 0.
        return 1. - max(diff, -1. * diff)/mean([value_a, value_b])
    elif type(value_a) is str and type(value_b) is str:
        N_max = max(len(value_a), len(value_b))
        if N_max == 0:
            return 0.
        return (N_max - editdistance.eval(value_a, value_b))/N_max
    elif type(value_a) is dict and type(value_b) is dict:
        fields = set(value_a.keys()) | set(value_b.keys())
        scores = [_similarity(value_a.get(field, None),
                              value_b.get(field, None)) for field in fields]
        return mean(scores)
    elif type(value_a) is list and type(value_b) is list:
        aligned = align_records({'a': value_a, 'b': value_b})
        scores = []
        for item in aligned:
            if len(item) != 2:
                scores.append(0.)
                continue
            item = dict(item)
            scores.append(_similarity(item['a'], item['b']))
        return mean(scores)
    if type(value_a) != type(value_b):
        logger.debug('Comparing objects with different types')
    return 0.


def _prep_value(value: object) -> object:
    """Ensure that ``value`` is hashable."""
    if value.__hash__ is None:
        return str(value)
    return value


def _cast_value(value: object) -> object:
    """Retrieve the original value type."""
    if type(value) is not str:
        return value
    if (value.startswith('[') and value.endswith(']')) or \
       (value.startswith('{') and value.endswith('}')):
        return eval(value)
    return value


def _pool(metadata: dict, fields: list, prob_valid: object,
          similarity_threshold: float=0.9) -> dict:
    """Pool similar values for a field across extractions."""
    # Similar values (above a threshold) for fields are grouped together, and
    #  their P(value|extractor, field) are combined (summed, then normalized).
    pooled = defaultdict(Counter)
    for extractor, metadatum in metadata.items():
        for field in fields:
            value = _prep_value(metadatum.get(field, None))
            if value is None:
                continue
            p_value = prob_valid(extractor, field)
            match = False
            for prev_value in list(pooled[field].keys()):
                if _similarity(value, prev_value) >= similarity_threshold:
                    p_prev = pooled[field][prev_value]
                    # Given that there can be same variation in values here,
                    #  if we encounter a substantially better value we should
                    #  use it instead.
                    if p_value > p_prev and value != prev_value:
                        # New assignment inherits all of the previous weight.
                        pooled[field][value] += p_value + p_prev
                        del pooled[field][prev_value]   # Cleanup.
                    else:
                        pooled[field][prev_value] += p_value
                    match = True
            if not match:
                pooled[field][value] += p_value
    # Return a native dict for cleanliness' sake.
    return {field: {value: score for value, score in scores.items()}
            for field, scores in pooled.items()}


def _select(pooled: dict) -> tuple:
    """Select the most likely values given their pooled weights."""
    result = {}
    max_probs = []
    for field, counts in pooled.items():
        # Feature-normalize accross distinct values.
        if len(counts) == 0:
            continue
        try:
            values, norm_prob = zip(*[(value, count/sum(counts.values()))
                                      for value, count in counts.items()
                                      if sum(counts.values()) > 0])
        except ValueError as e:
            continue
        result[field] = _cast_value(values[argmax(norm_prob)])
        max_probs.append(max(norm_prob))
    return result, _score(result)*mean(max_probs)


def _score(result: dict) -> float:
    """Evaluate the overall quality of the reference."""
    identifiers = [ident.get('identifier_type') for ident
                   in result.get('identifiers', [])]
    if result.get('doi') or 'arxiv' in identifiers:
        return 1.0
    core = ['volume', 'source', 'year', 'title', 'authors']
    return mean([1. if result.get(field) else 0. for field in core])


def arbitrate(metadata: list, valid: list, priors: list,
              similarity_threshold: float=0.9) -> dict:
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
    similarity_threshold : float
        Minimum similarity (0.-1.) to consider two values identical.

    Returns
    -------
    dict
        Authoritative metadata record for a single extracted reference.
    """
    # Kind of hoakie to coerce to dict, but it does make some things easier.
    metadata = dict(metadata)
    valid = dict(valid)
    priors = dict(priors)
    extractors = list(metadata.keys())

    _validate(extractors, priors, metadata, valid)

    # We want to know all of the unique field names present across the aligned
    #  extractions.
    fields = list({
        field for metadatum in metadata.values() for field in metadatum.keys()
    })

    # Pulling this out for readability.
    def _prob_valid(extractor: str, field: str) -> float:
        """The probability that the value for ``field`` is correct."""
        p_val = valid[extractor].get(field, 0.)
        p_extr = priors[extractor].get('__all__', 0.)
        p_extr_field = priors[extractor].get(field, p_extr)
        return p_val * p_extr_field

    pooled = _pool(metadata, fields, _prob_valid,
                   similarity_threshold=similarity_threshold)

    # Here we select the value with the highest P for each field.
    return _select(pooled)


def arbitrate_all(metadata_all: list, valid_all: list,
                  priors_all: list, N_extractions: int=0) -> list:
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
    N_extractions : int

    Returns
    -------
    list
        Optimal metadata for cited references. Each item is a ``dict``. See
        :func:`.arbitrate` for more details.
    """
    N = len(metadata_all)
    return list(map(arbitrate, metadata_all, valid_all, repeat(priors_all, N)))
