"""Normalize extracted references."""

from references import logging
from statistics import mean, median
from decimal import Decimal
import re
logger = logging.getLogger(__name__)


def _remove_dots(string: str) -> str:
    """Remove dots while preserving whitespace."""
    return re.sub(r"\.\s*", " ", string).strip()


def _remove_dots_from_author_names(author: dict) -> dict:
    """Remove dots from givennames and fullname."""
    givennames = author.get('givennames')
    fullname = author.get('fullname')
    if givennames is not None:
        author.update({'givennames': _remove_dots(givennames).title()})
    if fullname is not None:
        author.update({'fullname': _remove_dots(fullname).title()})
    return author


def _remove_leading_trailing_nonalpha(string: str) -> str:
    """Remove leading or trailing non-alphanumeric characters."""
    return re.sub(r"[^0-9a-zA-Z]+$", "", re.sub(r"^[^0-9a-zA-Z]+", "", string))


categories = [
    "acc-phys", "adap-org", "alg-geom", "ao-sci", "astro-ph", "atom-ph",
    "bayes-an", "chao-dyn", "chem-ph", "cmp-lg", "comp-gas", "cond-mat", "cs",
    "dg-ga", "funct-an", "gr-qc", "hep-ex", "hep-lat", "hep-ph", "hep-th",
    "math-ph", "mtrl-th", "math", "nlin", "nucl-ex", "nucl-th", "patt-sol",
    "physics", "plasm-ph", "q-alg", "q-bio", "quant-ph", "solv-int", "supr-con"
]


def _fix_arxiv_id(value: object) -> object:
    """Fix common mistakes in arXiv identifiers."""
    if isinstance(value, list):
        return [_fix_arxiv_id(obj) for obj in value]
    for category in categories:
        typo = category.replace('-', '')
        if typo in value:
            return value.replace(typo, category)
    return value


NORMALIZERS = [
    ('authors', _remove_dots_from_author_names),
    ('title', _remove_leading_trailing_nonalpha),
    ('source', lambda string: _remove_dots(string).title()),
    ('arxiv_id', _fix_arxiv_id)
]


def _normalize_record(record: dict) -> dict:
    """
    Perform normalization/cleanup on a per-field basis.

    Parameters
    ----------
    record : dict

    Returns
    -------
    dict
    """
    for field, normalizer in NORMALIZERS:
        value = record.get(field)
        if value is None:
            continue
        if isinstance(value, list):
            record[field] = [normalizer(obj) for obj in value]
        else:
            record[field] = normalizer(value)
    return record


def normalize_records(records: list) -> list:
    """
    Perform normalization/cleanup on a per-field basis.

    Parameters
    ----------
    records : list

    Returns
    -------
    list
    """
    return [_normalize_record(record) for record in records]


def filter_records(records: list, threshold: float=0.5) -> tuple:
    """
    Remove low-quality extracted references, and generate a composite score.

    Parameters
    ----------
    records : list
        Items are two-tuples of metadata (``dict``) and record score
        (``float``).
    threshold : float
        Minimum record score to retain.

    Return
    ------
    tuple
        Filtered list of reference metadata (``dict``) and a composite score
        for all retained records (``float``).
    """
    filtered_records = [
        (dict(list(rec.items()) + [('score', Decimal(str(round(sc, 2))))]), sc)
        for rec, sc in records if sc >= threshold
    ]
    if len(filtered_records) == 0:
        return [], 0.
    filtered_records, scores = zip(*filtered_records)
    return list(filtered_records), mean(scores)
