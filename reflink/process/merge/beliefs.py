import os
import re
from functools import partial

from reflink.process.textutil import clean_text
from reflink.process.extract.regex_arxiv import REGEX_ARXIV_STRICT
from reflink.process.extract.regex_identifiers import (
    REGEX_DOI, REGEX_ISBN_10, REGEX_ISBN_13
)

try:
    from array import array
    from pybloof import StringBloomFilter
except ImportError as e:
    StringBloomFilter = None


def _prepare_filters_or_not():
    try:
        return _load_filters()
    except Exception as e:
        return {}


def _load_filters():
    # a size bigger than our filters but smaller than memory so we can
    # just automatically load the whole thing without recording sizes
    BIGNUMBER = int(1e8)

    stubs = ['auth', 'title']
    bloom_files = [
        os.path.join(os.environ.get('REFLINK_DATA_DIRECTORY', '.'), i)
        for i in ['words_bloom_filter_{}.bytes'.format(j) for j in stubs]
    ]

    bloom_filters = {}
    for stub, filename in zip(stubs, bloom_files):
        with open(filename, 'rb') as fn:
            try:
                arr = array('b')
                arr.fromfile(fn, BIGNUMBER)
            except EOFError as e:
                # we expect this to happen, we asked for way to many bytes, but
                # it doesn't matter, we've successfully filled our array
                pass
            bfilter = StringBloomFilter.from_byte_array(arr)
            bloom_filters[stub] = bfilter

    return bloom_filters


def bloom_match(value: str, bloom_filter: StringBloomFilter) -> float:
    score = [
        1 if word in bloom_filter else 0 for word in clean_text(value).split()
    ]
    return sum(score) / len(score)


def is_integer(value: str) -> float:
    try:
        int(value)
    except ValueError as e:
        return 0.0
    return 1.0


def is_integer_like(value: str) -> float:
    numbers = re.findall(r'(?:\s+)?(\d+)(?:\s+)?', value)
    if any([is_integer(i) for i in numbers]):
        return 1.0
    return 0.0


def is_year(value: str) -> float:
    try:
        year = int(value)
        if year > 1600 and year < 2100:
            return 1.0
    except ValueError as e:
        return 0.0
    return 0.0


def is_year_like(value: str) -> float:
    numbers = re.findall(r'(?:\s+)?(\d+)(?:\s+)?', value)
    if any([is_year(i) for i in numbers]):
        return 1.0
    return 0.0


def is_pages(value: str) -> float:
    pages = re.compile(r'(\d+)(?:\s+)?[\s\-._/\:]+(?:\s+)?(\d+)')
    match = pages.match(value)

    if match:
        start, end = [int(i) for i in match.groups()]
        if start < end:
            return 1.0
        return 0.5
    return 0.0


def valid_doi(value: str) -> float:
    if re.match(REGEX_DOI, value):
        return 1.0
    return 0.0


def valid_identifier(value: list) -> float:
    num_identifiers = len(value)
    num_good = 0

    for ID in value:
        idtype = ID.get('identifier_type', '')
        idvalue = ID.get('identifier', '')

        if idtype == 'arxiv':
            if re.match(REGEX_ARXIV_STRICT, idvalue):
                num_good += 1

        if idtype == 'isbn':
            if re.match(REGEX_ISBN_10, idvalue):
                num_good += 1
            elif re.match(REGEX_ISBN_13, idvalue):
                num_good += 1

    return num_good / num_identifiers


def unity(r):
    return 1.0


bloom_filters = _prepare_filters_or_not()
if StringBloomFilter and bloom_filters:
    words_title = partial(bloom_match, bloom_filter=bloom_filters['title'])
    words_auth = partial(bloom_match, bloom_filter=bloom_filters['auth'])
else:
    words_title = unity
    words_auth = unity

BELIEF_FUNCTIONS = {
    'title': [words_title],
    'raw': [words_auth, words_title],
    'authors': [words_auth],
    'doi': [valid_doi],
    'volume': unity,
    'issue': unity,
    'pages': [is_integer_like, is_pages],
    'source': unity,
    'year': [is_integer, is_integer_like, is_year, is_year_like],
    'identifiers': [valid_identifier]
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
