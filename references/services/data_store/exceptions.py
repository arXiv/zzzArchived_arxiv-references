"""Exceptions raised by :mod:`references.services.data_store`."""


class ReferencesNotFound(Exception):
    """A request was made for references that do not exist."""


class CommunicationError(ConnectionError):
    """There was a problem communicating with Redis."""
