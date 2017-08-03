import copy
import statistics
from itertools import islice, chain
from typing import List

from reflink.process.textutil import clean_text


def argmax(array):
    index, value = max(enumerate(array), key=lambda x: x[1])
    return index


def jacard(str0: str, str1: str) -> float:
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


def digest(metadata: dict) -> str:
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
    """
    Flatten a structure into a list of the values in that structure
    """
    if isinstance(arr, dict):
        return list(chain(*[flatten(i) for i in arr.values()]))
    if isinstance(arr, list):
        return list(chain(*[flatten(i) for i in arr]))
    return [arr]


def similarity_cutoff(records: List[dict]) -> float:
    """
    Get the similarity score cutoff to assist in determining whether two items
    are actually similar or in fact have no matches.

    Parameters
    ----------
    records : dict
        A set of records where each reference list is labelled by the extractor
        name i.e. {"cermine": [references], "scienceparse": [references]}

    Returns
    -------
    cutoff : float
        The similarity score cutoff value
    """
    def _cutoff(data):
        # median absolute deviation (MAD)
        median = statistics.median(data)
        mad = 1.4826 * statistics.median([abs(d - median) for d in data])
        return median + 3*mad

    def _jacard_matrix(r0, r1, num):
        # calculate all-all jacard similarity
        # matrix for two sets of records
        out = [[0]*num for i in range(num)]
        for i in range(min(num, len(r0))):
            for j in range(min(num, len(r1))):
                out[i][j] = jacard(
                    digest(r0[i]),
                    digest(r1[j])
                )
        return out

    R = len(records)
    N = max(map(len, records))

    # get the full jacard matrix to get the cutoff values first. unfortunately,
    # its difficult to make use of these values later, but they are vital to
    # calculating the cutoff
    jac = {}
    for i, rec0 in islice(enumerate(records), 0, R-1):
        for j, rec1 in islice(enumerate(records), i+1, R):
            jac[i, j] = _jacard_matrix(rec0, rec1, N)

    return _cutoff(flatten(copy.deepcopy(jac)))


def align_records(records: List[dict]) -> List[List[tuple]]:
    """
    Match references from a number of records so that similar

    Parameters
    ----------
    records : dict of metadata
        A set of records where each reference list is labelled by the extractor
        name i.e. {"cermine": [references], "scienceparse": [references]}

    Returns
    -------
    aligned_references : list of list of tuples (dict, string)
        Structure of returned data:
            [
                [
                    (extractor 1, {reference 1}),
                    (extractor 2, {reference 1}),
                ],
                [
                    (extractor 3, {reference 2}),
                ],
                [
                    (extractor 1, {reference 3}),
                    (extractor 2, {reference 3}),
                    (extractor 3, {reference 3}),
                ]
            ]

    """
    def _jacard_max(r0, rlist):
        # calculate the maximum jacard score between r0 and the list rlist
        return max([jacard(digest(r0), digest(r1)) for r1 in rlist])

    cutoff = similarity_cutoff(records)

    # pairwise integrate the lists together, keeping the output list as the
    # master record as we go. 0+1 -> 01, 01+2 -> 012 ...
    keys = list(records.keys())
    output = [[(keys[0], rec)] for rec in records[keys[0]]]
    for ikey, key in islice(enumerate(keys), 1, len(records)):
        used = []

        record = records[key]
        for iref, ref in enumerate(record):
            # Create a list of possible indices in the output onto which we
            # will map the current reference. only keep those above the cutoff.
            # keep track of the indices to only use each once
            # FIXME -- maybe we don't want to do greedy descent (instead global
            # optimization of scores for all references at once, but that is
            # combinatorial and needs to have careful algorithms)
            scores = []
            for iout, out in enumerate(output):
                score = _jacard_max(ref, [l[1] for l in out])
                if score <= cutoff:
                    continue
                scores.append((score, iout))

            scores = [
                (score, index) for score, index in reversed(sorted(scores))
                if index not in used
            ]

            entry = [(key, ref)]
            if scores:
                score, index = scores[0]
                output[index] = output[index] + entry
            else:
                output.append(entry)

    return output
