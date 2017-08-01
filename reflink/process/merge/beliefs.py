import re
#from reflink.process.extract import 


def is_integer_like(value: str) -> float:
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
    'title': unity,
    'raw': unity,
    'authors': unity,
    'doi': unity,
    'volume': unity,
    'issue': unity,
    'pages': unity,
    'source': unity,
    'year': [is_integer_like, is_integer, is_year],
    'identifiers': unity 
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
