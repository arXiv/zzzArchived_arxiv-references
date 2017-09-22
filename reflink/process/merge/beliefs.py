import os
import re
from functools import partial

from reflink.types import Callable
from reflink import logging
from reflink.process.textutil import clean_text
from reflink.process.extract.regex_arxiv import REGEX_ARXIV_STRICT
from reflink.process.extract.regex_identifiers import (
    REGEX_DOI, REGEX_ISBN_10, REGEX_ISBN_13
)

RE_INTEGER = (
    r'(?:^|(?:\s+))'
    r'(\d+)'
    r'(?:$|(?:\s+))'
)


try:
    from array import array
    from pybloof import StringBloomFilter
except ImportError as e:
    StringBloomFilter = None


logger = logging.getLogger(__name__)


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
        os.path.join(os.environ.get('REFLINK_DATA_DIRECTORY', './data'), i)
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
        1 if word in bloom_filter else 0
        for word in clean_text(value, numok=True).split()
    ]
    if len(score) == 0:
        return 0.0
    return sum(score) / len(score)


def minimum_length(length: int) -> Callable:
    def _min_len(value: object) -> float:
        if length > len(value) > 0:
            return 0.
        return 1.
    return _min_len


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
        int(value)
    except ValueError as e:
        return 0.0
    return 1.0


def is_integer_like(value: object) -> float:
    if isinstance(value, int):
        return 1.0
    if len(value) == 0:
        return 0.0

    numbers = re.findall(RE_INTEGER, value)
    leftovers = re.subn(RE_INTEGER, '', value)[0]

    if not numbers:
        return 0.0

    return (
        (sum([is_integer(i) for i in numbers])/len(numbers)) *
        ((len(value) - len(leftovers)) / len(value))
    )


def is_year(value: str) -> float:
    try:
        year = int(value)
        if year > 1600 and year < 2100:
            return 1.0
    except ValueError as e:
        return 0.0
    return 0.0


def is_year_like(value: str) -> float:
    try:
        numbers = re.findall(RE_INTEGER, value)
        if not numbers:
            return 0.0
        return (1. * sum([is_year(i) for i in numbers]))/len(numbers)
    except Exception as e:
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

        if idtype == 'isbn':
            if re.match(REGEX_ISBN_10, idvalue):
                num_good += 1
            elif re.match(REGEX_ISBN_13, idvalue):
                num_good += 1

    return num_good / num_identifiers


def valid_arxiv_id(value: str) -> float:
    if re.match(REGEX_ARXIV_STRICT, value):
        return 1.0
    return 0.0


def unity(r):
    return 1.0


bloom_filters = _prepare_filters_or_not()
if StringBloomFilter and bloom_filters:
    words_title = partial(bloom_match, bloom_filter=bloom_filters['title'])
    words_auth = partial(bloom_match, bloom_filter=bloom_filters['auth'])
else:
    words_title = unity
    words_auth = unity


def words_author_structure(value: list) -> float:
    num_authors = 0
    num_good = 0.0

    for auth in value:
        name = clean_text(' '.join(auth.values()))
        mod = words_auth(name)
        surname = auth.get('surname')
        if surname:
            mod *= 1./len(surname.split())
        num_good += mod
        num_authors += 1

    if num_authors == 0:
        return 0.0
    return num_good / num_authors


BELIEF_FUNCTIONS = {
    'title': [words_title, minimum_length(5)],
    'raw': [words_title, words_auth],
    'authors': [words_author_structure],
    'doi': [valid_doi, contains('.'), contains('/'), doesnt_end_with('-')],
    'volume': [likely(is_integer_like, min_prob=0.8)],
    'pages': [is_integer_like, is_pages],
    'source': [does_not_contain_arxiv],
    'year': [is_integer_like, is_integer, is_year_like, is_year],
    'identifiers': [valid_identifier],
    'arxiv': [valid_arxiv_id]
}


def calculate_belief(reference: dict) -> dict:
    """
    Calculate the beliefs about the elements in a single record.

    Generates a data structure similar to the input but with the values
    replaced by probabilities (float in 0.0-1.0).

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
        if not value:
            # Blank values are perfectly plausible, and there isn't much else
            # that we can say about them.
            output[key] = 1.
            continue
        funcs = BELIEF_FUNCTIONS.get(key, [unity])
        score = 0.
        for func in funcs:
            # We don't want the whole process to get derailed when one
            #  function fails.
            try:
                score += func(value)
            except Exception as e:
                logger.error('Validation function for %s failed with: %s' %
                             (key, e))
        output[key] = score/len(funcs)

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
