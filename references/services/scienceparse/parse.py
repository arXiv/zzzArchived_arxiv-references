"""Business logic for processing ScienceParse extracted references."""

import os
from typing import List

from arxiv.base import logging
from arxiv.status import HTTP_200_OK

from references.domain import Reference, ReferenceSet, Author
from references.services import scienceparse

logger = logging.getLogger(__name__)


def parse_auth_line(text: str) -> List[str]:
    """
    Very simple attempt to split an author line into first / last names.
    At the moment, this only uses spaces, but could be more involved later
    """
    words = text.split(' ')
    first, last = words[:-1], words[-1:]
    return [' '.join(part) for part in [first, last]]


def format_scienceparse_output(output: dict) -> List[Reference]:
    """
    Generate :class:`.Reference`s from ScienceParse output.

    Parameters
    ----------
    output : dict
        The output of the ScienceParse API call, structured dict of metadata

    Returns
    -------
    metadata : list
        List of :class:`.Reference` instances.
    """
    if 'references' not in output:
        msg = 'ScienceParse output does not contain references'
        logger.error(msg)
        raise KeyError(msg)

    references = []
    for ref in output['references']:
        authors = []
        for auth in ref.get('authors', []):
            if auth:
                authors.append(parse_auth_line(auth))
        authors = [Author(givennames=first, surname=last)   # type: ignore
                   for first, last in authors]
        reference = Reference(    # type: ignore
            title=ref.get('title'),
            year=str(ref.get('year')),
            source=ref.get('venue'),
            authors=authors
        )
        references.append(reference)

    return references
