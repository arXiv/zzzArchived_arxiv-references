import statistics
from itertools import islice, chain
from typing import List

from reflink.process.textutil import clean_text


def argmax(array):
    index, value = max(enumerate(array), key=lambda x: x[1])
    return index


def jacard(str0, str1):
    """
    Jacard similarity score between str0 and str1, containing the fraction of
    matching words that are the same between the two strings.

    Parameters
    ----------
    str0 : str
    str1 : str

    Returns
    -------
    similarity : float
        The jacard similarity
    """
    words0 = set(str0.split())
    words1 = set(str1.split())
    shared_words = len(words0.intersection(words1))
    all_words = len(words0.union(words1))

    if all_words == 0:
        return 0.0
    return shared_words/all_words


def digest(metadata):
    """
    Create a single string that represents the record. It does so by
    recursively digesting the structure, taking any strings in a list or
    dictionary value and combining them into a word list (single string)
    
    Parameters
    ----------
    metadata : dict
        Single record. Does not necessarily have to be a dict, but that is
        what we are working with at the moment

    Returns
    -------
    digest : string
    """
    badkeys = ['raw', 'doi', 'identifiers']

    if isinstance(metadata, list):
        return clean_text(
            ' '.join([digest(l) for l in metadata]), numok=True
        )
    elif isinstance(metadata, dict):
        return clean_text(
            ' '.join([
                digest(v) for k, v in metadata.items() if k not in badkeys
            ]), numok=True
        )
    else:
        return clean_text(str(metadata), numok=True)


def flatten(arr):
    if isinstance(arr, list):
        return list(chain(*[flatten(i) for i in arr]))
    return [arr]


def align_records(records):
    """

    """
    def _cutoff(data):
        # median absolute deviation (MAD)
        median = statistics.median(data)
        mad = 1.4826 * statistics.median([abs(d - median) for d in data])
        return median + 3*mad

    def _jacard_matrix(r0, r1, num):
        # calculate all-all jacard similarity
        # matrix for two sets of records
        out = [[0]*N for i in range(N)]
        for i in range(min(num, len(r0))):
            for j in range(min(num, len(r1))):
                out[i][j] = jacard(
                    digest(r0[i]),
                    digest(r1[j])
                )
        return out

    R = len(records)
    N = max(map(len, records))

    jac = [[0]*R for j in range(R)]
    ind = [[0]*R for j in range(R)]

    # get the full jacard matrix to get the cutoff values first
    for i, rec0 in islice(enumerate(records), 0, R):
        for  j, rec1 in islice(enumerate(records), 0, R):
            jac[i][j] = _jacard_matrix(rec0, rec1, N)

    cutoff = _cutoff(flatten(jac))

    # use the jacard matrix to get the index mappings
    for i, rec0 in islice(enumerate(records), 0, R):
        for  j, rec1 in islice(enumerate(records), 0, R):
            ind[i][j] = [argmax(v) for v in jac[i][j] if max(v) > cutoff]

    output = []

    return jac, ind


def consolidate_records(records: List[List[dict]]) -> dict:
    """
    Takes a list of reference metadata records (each formatted according to the
    schema) and reconciles them with each other to form one primary record for
    each item. First step is to match the lists against each other using
    similarity measures. Then, for each individual record we combine the
    possible fields and augment them with possible external information to form
    a single record.

    Parameters
    ----------
    records : list of list of reference metadata
        The reference records from multiple extraction servies / lookup services

    Returns
    -------
    united : list of dict (reference data)
    """
    matched_records = []
