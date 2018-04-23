"""Persistance for extracted references, using Redis."""

import json
from typing import List, Tuple, Optional
from functools import wraps

import redis

from arxiv.base.globals import get_application_config, get_application_global
from references.domain import Reference, ReferenceSet
from .exceptions import CommunicationError, ReferencesNotFound


class ReferenceStoreSession(object):
    """Manages a connection to Redis."""

    def __init__(self, host: str, port: int, database: int) -> None:
        """Open the connection to Redis."""
        self.r = redis.StrictRedis(host=host, port=port, db=database)

    def _version(self, rset: ReferenceSet) -> str:
        """Generate a key using document ID, version, and extrator."""
        return f"{rset.document_id}_{rset.version}_{rset.extractor}"

    def _extractor(self, rset: ReferenceSet) -> str:
        """Generate an indexing key based on document ID and extractor."""
        return f"{rset.document_id}_{rset.extractor}"

    def _index(self, rset: ReferenceSet) -> None:
        """Update document indices."""
        # We're using a sorted sets as a kind of index. For each
        # document-extraction key, we index the versioned extractions using
        # the version number itself (major and minor parts) as the sort score.
        version = float('.'.join(rset.version.split('.')[:2]))
        self.r.zadd(self._extractor(rset), version, self._version(rset))

    def save(self, reference_set: ReferenceSet) -> None:
        """Store a :class:`.ReferenceSet`."""
        try:
            self.r.set(self._version(reference_set), json.dumps(reference_set))
        except redis.exceptions.ConnectionError as e:
            raise CommunicationError('Failed to save references') from e

    def load(self, document_id: str, extractor: str = 'combined',
             version: str = 'latest') -> ReferenceSet:
        """Load a class:`.ReferenceSet` from the data store."""
        try:
            if version == 'latest':
                _extractor = f"{document_id}_{extractor}"
                key = self.r.zrangebyscore(_extractor, '-inf', '+inf')[-1]
            else:
                key = f"{document_id}_{version}_{extractor}"
            data = self.r.get(key)
        except redis.exceptions.ConnectionError as e:
            raise CommunicationError('Failed to load references') from e
        if not data:
            raise ReferencesNotFound('No such extraction')
        return ReferenceSet(**json.loads(data))     # type: ignore


def init_app(app: object) -> None:
    """
    Set default configuration parameters on an application.

    Parameters
    ----------
    app : :class:`flask.Flask`
    """
    config = get_application_config(app)
    config.setdefault('REFERENCES_REDIS_HOST', 'localhost')
    config.setdefault('REFERENCES_REDIS_PORT', '6379')
    config.setdefault('REFERENCES_REDIS_DATABASE', '1')


def get_session(app: object = None) -> ReferenceStoreSession:
    """
    Initialize a session with the data store.

    Parameters
    ----------
    app : :class:`flask.Flask`
        If not provided, will attempt to get the current application.

    Returns
    -------
    :class:`.DataStoreSession`
    """
    config = get_application_config(app)
    host = config.get('REFERENCES_REDIS_HOST', 'localhost')
    port = int(config.get('REFERENCES_REDIS_PORT', '6379'))
    database = int(config.get('REFERENCES_REDIS_DATABASE', '1'))
    return ReferenceStoreSession(host, port, database)


def current_session() -> ReferenceStoreSession:
    """Get/create :class:`.ReferenceStoreSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'data_store' not in g:
        g.data_store = get_session()
    session: ReferenceStoreSession = g.data_store
    return session


@wraps(ReferenceStoreSession.save)
def save(references: ReferenceSet) -> None:
    """
    Store extracted references for a document.

    Parameters
    -------
    references : :class:`.ReferenceSet`

    """
    session = current_session()
    return session.save(references)


@wraps(ReferenceStoreSession.load)
def load(document_id: str, extractor: str = 'combined',
         version: str = 'latest') -> ReferenceSet:
    """
    Retrieve extracted references.

    Parameters
    ----------
    document_id : str
        arXiv paper ID (with version affix).
    extractor : str
        If provided, load the raw extraction for a particular extractor.


    Returns
    -------
    :class:`.ReferenceSet`

    """
    return current_session().load(document_id, extractor=extractor)
