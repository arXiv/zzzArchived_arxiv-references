import re
from reflink.types import Callable
#from reflink.process.extract import


def likely(func, min_prob: float=0.0, max_prob: float=1.0) -> Callable:
    def call(value: object) -> float:
        return max(min(func(value), max_prob), min_prob)
    return call


def does_not_contain_arxiv(value: object) -> float:
    if not isinstance(value, str):
        return 0.0
    return 0. if 'arxiv' in value else 1.


def is_integer_like(value: object) -> float:
    if value is None:
        return 0.
    if isinstance(value, int):
        return 1.0
    if len(value) == 0:
        return 0.0
    numbers = re.findall(r'(?:\s+)?(\d+)(?:\s+)?', value)
    return (1. * sum([is_integer(i) for i in numbers]))/len(value)


def contains(substring: str, false_prob: float=0.0,
             true_prob: float=1.0) -> Callable:
    def call(value: object) -> float:
        if not isinstance(value, str):
            return 0.0
        return true_prob if substring in value else false_prob
    return call


def ends_with(substring: str, false_prob: float=0.0,
             true_prob: float=1.0) -> Callable:
    def call(value: object) -> float:
        if not isinstance(value, str):
            return 0.0
        return true_prob if value.endswith(substring) else false_prob
    return call


def doesnt_end_with(substring: str, false_prob: float=0.0,
                    true_prob: float=1.0) -> Callable:
    return ends_with(substring, false_prob=true_prob, true_prob=false_prob)


def is_integer(value: str) -> float:
    try:
        number = int(value)
    except ValueError as e:
        return 0.0
    return 1.0


def is_year(value: str) -> float:
    try:
        year = int(value)
        if year > 1600 and year < 2100:
            return 1.0
    except ValueError as e:
        return 0.0
    return 0.0


def unity(r):
    return 1.0


BELIEF_FUNCTIONS = {
    'title': [unity],
    'raw': [unity],
    'authors': [unity],
    'doi': [contains('.'), contains('/'), doesnt_end_with('-')],
    'volume': [likely(is_integer_like, min_prob=0.5)],
    'issue': [likely(is_integer_like, min_prob=0.5)],
    'pages': [unity],
    'source': [does_not_contain_arxiv],
    'year': [is_integer_like, is_integer, is_year],
    'identifiers': [unity]
}


def calculate_belief(reference: dict) -> dict:
    """
    Calculate the beliefs about the elements in a single record, returning a
    data structure similar to the input but with the values replaced by
    probabilities.

    Parameters
    ----------
    reference : dict
        A single reference metadata dictionary (formatted according to schema)

    Returns
    -------
    beliefs : dict
        The same structure as the input but with probabilities instead of
        the values that came in
    """
    output = {}

    for key, value in reference.items():
        funcs = BELIEF_FUNCTIONS.get(key, [unity])
        output[key] = sum([func(value) for func in funcs]) / len(funcs)

    return output


def identity_belief(reference: dict) -> dict:
    """
    Returns an identity function for the beliefs (so that we can debug more
    easily). Therefore, does almost nothing but replace values with 1.0 in
    a data structure.

    Parameters
    ----------
    reference : dict
        A single reference metadata dictionary

    Returns
    -------
    beliefs : dict
        The beliefs about the values within a record, all unity
    """
    return {key: unity(value) for key, value in reference.keys()}


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
            (extractor, calculate_belief(metadatum))
            for extractor, metadatum in reference
        ] for reference in aligned_references
    ]
