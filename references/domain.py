from typing import NamedTuple, List, Optional


class Author(NamedTuple):
    """A parsed author name in a bibliographic reference."""

    surname: str = ''
    givennames: str = ''
    prefix: str = ''
    suffix: str = ''
    fullname: str = ''


class Identifier(NamedTuple):
    """A persistent identifier for a cited reference."""

    identifer_type: str
    """E.g. ISBN, ISSN, URI."""
    identifier: str


class ExtractedReference(NamedTuple):
    """An instance of a parsed bibliographic reference."""

    title: str = ''
    """The title of the reference."""
    raw: str = ''
    """The un-parsed reference string."""
    arxiv_id: str = ''
    """arXiv paper ID."""
    authors: List[Author] = []
    reftype: str = 'article'
    """The type of work to which the reference refers."""
    doi: str = ''
    volume: str = ''
    issue: str = ''
    pages: str = ''
    source: str = ''
    """Journal, conference, etc."""
    year: str = ''
    identifiers: List[Identifier] = []
