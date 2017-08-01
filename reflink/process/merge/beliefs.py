import re
#from reflink.process.extract import


def is_integer_like(value: object) -> float:
    if isinstance(value, int):
        return 1.0
    numbers = re.findall(r'(?:\s+)?(\d+)(?:\s+)?', value)
    if any([is_integer(i) for i in numbers]):
        return 1.0
    return 0.0


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
    'doi': [unity],
    'volume': [unity],
    'issue': [unity],
    'pages': [unity],
    'source': [unity],
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
