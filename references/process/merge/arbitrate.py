"""Generate authoritative reference metadata using validity probabilities."""

from collections import Counter, defaultdict
from statistics import mean
from itertools import repeat
import re

from typing import Tuple, Any, Union, Callable, List, Dict

import editdistance    # For string similarity.

from references.domain import Reference
from references.process.util import argmax
from arxiv.base import logging
from references.process.merge.align import align_records

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
    def _keysort(item):     # type: ignore
        return item[0]

    return str([(key.strip(), value.strip()) for key, value
                in sorted(value.items(), key=_keysort)])


def _validate(extractors: list, priors: dict, metadata: dict,
              valid: dict) -> None:
    """Check that extraction data is valid for arbitrartion."""
    try:
        missing = set(extractors) - set(priors.keys())
        assert len(missing) == 0
    except AssertionError as e:
        logger.error('Missing priors for %s',  '; '.join(list(missing)))
        raise ValueError('Priors missing for one or more extractors') from e
    try:
        assert len(metadata) == len(valid)
        assert len(set(metadata.keys()) - set(valid.keys())) == 0
    except AssertionError as e:
        msg = 'Metadata and validity objects must have the same shape'
        logger.error(msg)
        raise ValueError(msg) from e


def _similarity_int_float(value_a: Union[float, int],
                          value_b: Union[float, int]) -> float:
    """Relative similarity of two numeric values."""
    diff = float(value_a) - float(value_b)
    if mean([value_a, value_b]) == 0:
        return 0.
    return 1. - max(diff, -1. * diff)/mean([value_a, value_b])


def _similarity_str(value_a: str, value_b: str) -> float:
    """Relative similarity of two strings, based on edit distance."""
    N_max = max(len(value_a), len(value_b))
    if N_max == 0:
        return 0.
    sim: float = (N_max - editdistance.eval(value_a, value_b))/N_max
    return sim


def _similarity_dict(value_a: dict, value_b: dict) -> float:
    """Similarity of two dictionaries, based on shared keys and values."""
    fields = set(value_a.keys()) | set(value_b.keys())
    scores = [_similarity(value_a.get(field, None),
                          value_b.get(field, None)) for field in fields]
    return mean(scores)


def _similarity_list(value_a: list, value_b: list) -> float:
    """Similarity of two lists, based on values (without regard to order)."""
    aligned = align_records({'a': value_a, 'b': value_b})
    scores = []
    for item in aligned:
        if len(item) != 2:
            scores.append(0.)
            continue
        _item: dict = {k: v for k, v in item}
        scores.append(_similarity(_item['a'], _item['b']))
    return mean(scores)


def _similarity(value_a: Any, value_b: Any) -> float:
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
        return _similarity_int_float(value_a, value_b)
    elif type(value_a) is str and type(value_b) is str:
        return _similarity_str(value_a, value_b)
    elif type(value_a) is dict and type(value_b) is dict:
        return _similarity_dict(value_a, value_b)
    elif type(value_a) is list and type(value_b) is list:
        return _similarity_list(value_a, value_b)
    if type(value_a) != type(value_b):
        logger.debug('Comparing objects with different types')
    return 0.


def _prep_value(value: object) -> Any:
    """Ensure that ``value`` is hashable."""
    if value.__hash__ is None:
        return str(value)
    return value


def _cast_value(field: str, value: Union[str, int]) -> Any:
    """Retrieve the original value type."""
    if field == 'year':
        try:
            return int(value)
        except ValueError:    # Just in case we get something odd here.
            return None
    if field in ['authors', 'identifiers']:
        try:
            return list(eval(value))    # type: ignore
        except SyntaxError:
            return value
    return value


def _fix_authors(authors: list) -> list:
    """Fill out fullname if not set."""
    fixed = []
    for author in authors:
        try:
            givennames = author.get('givennames')
            surname = author.get('surname')
            fullname = author.get('fullname')
            if givennames and surname and not fullname:
                author['fullname'] = '%s %s' % (givennames, surname)
        except AttributeError:
            pass
        fixed.append(author)
    return fixed


def _pool(metadata: Dict[str, Reference], fields: list, prob_valid: Callable,
          similarity_threshold: float = 0.9) -> dict:
    """Pool similar values for a field across extractions."""
    # Similar values (above a threshold) for fields are grouped together, and
    #  their P(value|extractor, field) are combined (summed, then normalized).
    pooled: defaultdict = defaultdict(Counter)
    for extractor, metadatum in metadata.items():
        for field in fields:
            value = _prep_value(getattr(metadatum, field, None))
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


def _select(pooled: dict) -> Tuple[Reference, float]:
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
        result[field] = _cast_value(field, values[argmax(norm_prob)])
        if field == 'authors':
            result[field] = _fix_authors(result[field])
        max_probs.append(max(norm_prob))
    ref = Reference(**result)   # type: ignore
    return ref, _score(result) * mean(max_probs)


def _score(result: dict) -> float:
    """Evaluate the overall quality of the reference."""
    if result.get('doi') or result.get('arxiv'):
        return 1.0
    core = ['volume', 'source', 'year', 'authors']
    return mean([1. if result.get(field) else 0. for field in core])


def arbitrate(metadata: List[Tuple[str, Reference]], valid: list, priors: list,
              similarity_threshold: float = 0.9) -> Tuple[Reference, float]:
    """
    Apply arbitration logic to raw extraction metadata for a single reference.

    Parameters
    ----------
    metadata : list
        Each item is a two-tuple of ``(str, Reference)``,
        representing an extractor and its metadata record.
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
    :class:`.Reference`
        Authoritative metadata record for a single extracted reference.
    float
        Reference quality score.
    """
    # Kind of hoakie to coerce to dict, but it does make some things easier.
    _metadata = dict(metadata)
    _valid = dict(valid)
    _priors = dict(priors)
    _extractors = list(_metadata.keys())

    _validate(_extractors, _priors, _metadata, _valid)

    # We want to know all of the unique field names present across the aligned
    #  extractions.
    fields = list(set(Reference.__annotations__.keys())
                  - {'score', 'identifier'})

    # Pulling this out for readability.
    def _prob_valid(extractor: str, field: str) -> float:
        """The probability that the value for ``field`` is correct."""
        p_val = _valid[extractor].get(field, 0.)
        p_extr = _priors[extractor].get('__all__', 0.)
        p_extr_field = _priors[extractor].get(field, p_extr)
        p: float = p_val * p_extr_field
        return p

    pooled = _pool(_metadata, fields, _prob_valid,
                   similarity_threshold=similarity_threshold)
    # Here we select the value with the highest P for each field.
    return _select(pooled)


def arbitrate_all(metadata: List[List[Tuple[str, Reference]]],
                  valid: list, priors: list, N_extractions: int = 0) \
        -> List[Tuple[Reference, float]]:
    """
    Helper to apply arbitration to metadata for a set of cited references.

    Parameters
    ----------
    metadata : list
        List of lists (see :func:`.arbitrate`).
    valid : list
        List of lists (see :func:`.arbitrate`).
    priors : list
        List of lists (see :func:`.arbitrate`).
    N_extractions : int

    Returns
    -------
    list
        Optimal metadata for cited references. Each item is a ``dict``. See
        :func:`.arbitrate` for more details.
    """
    N = len(metadata)
    priors_iter = repeat(priors, N)
    return list(map(arbitrate, metadata, valid, priors_iter))  # type: ignore
