"""Core data structures in the references application."""

from typing import List, Optional
from datetime import datetime
from base64 import b64encode
from dataclasses import dataclass, field, asdict
from unidecode import unidecode


@dataclass
class Author:
    """A parsed author name in a bibliographic reference."""

    surname: str = field(default_factory=str)
    givennames: str = field(default_factory=str)
    prefix: str = field(default_factory=str)
    suffix: str = field(default_factory=str)
    fullname: str = field(default_factory=str)


@dataclass
class Identifier:
    """A persistent identifier for a cited reference."""

    identifer_type: str
    """E.g. ISBN, ISSN, URI."""
    identifier: str


@dataclass
class Reference:
    """An instance of a parsed bibliographic reference."""

    title: Optional[str] = field(default=None)
    """The title of the reference."""
    raw: str = field(default_factory=str)
    """The un-parsed reference string."""
    arxiv_id: Optional[str] = field(default=None)
    """arXiv paper ID."""
    authors: List[Author] = field(default_factory=list)
    reftype: str = field(default='article')
    """The type of work to which the reference refers."""
    doi: Optional[str] = field(default=None)
    volume: Optional[str] = field(default=None)
    issue: Optional[str] = field(default=None)
    pages: Optional[str] = field(default=None)
    source: Optional[str] = field(default=None)
    """Journal, conference, etc."""
    year: Optional[str] = field(default=None)
    identifiers: List[Identifier] = field(default_factory=list)

    identifier: str = field(default_factory=str)
    """Unique identifier for this extracted reference."""

    score: float = field(default=0.)

    def __post_init__(self) -> None:
        """Set the identifier based on reference content."""
        hash_string = bytes(unidecode(self.raw), encoding='ascii')
        self.identifier = str(b64encode(hash_string), encoding='utf-8')[:100]

    def to_dict(self) -> dict:
        """Return a dict representation of this object."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ReferenceSet:
    """A collection of :class:`.Reference`."""

    document_id: str
    """arXiv paper ID (with version affix)."""
    references: List[Reference]
    version: str
    """Version of this application."""
    score: float
    """In the range 0-1; relative quality of the set as a whole."""
    created: datetime
    updated: datetime
    extractor: str = 'combined'
    """
    Name of the extractor used.

    Default is combined (for reconciled reference set). May also be 'author',
    for the author-curated set.
    """
    extractors: List[str] = field(default_factory=list)
    """Extractors used to generate this reference set."""
    raw: bool = field(default=False)
    """If True, refs are from a single extractor before reconciliation."""

    def to_dict(self) -> dict:
        """Generate a dict representation of this object."""
        data: dict = asdict(self)
        data['created'] = self.created.isoformat()
        data['updated'] = self.updated.isoformat()
        return data
