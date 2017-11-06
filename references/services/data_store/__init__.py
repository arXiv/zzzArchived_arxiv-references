"""Integration with reference metadata store."""

import os

from flask import _app_ctx_stack as stack
from flask import current_app
from references import logging
from typing import List
from references.context import get_application_config, get_application_global
from .raw import RawExtractionSession
from .references import ReferenceSession
from .extractions import ExtractionSession


ReferenceData = List[dict]

logger = logging.getLogger(__name__)


class DataStoreSession(object):
    """Container for datastore sessions."""

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, aws_session_token: str, region_name: str,
                 verify: bool=True, stored_schema: str=None,
                 extracted_schema: str=None, raw_table_name: str=None,
                 extractions_table_name: str=None,
                 references_table_name: str=None) -> None:
        """Initialize datastore sessions."""
        raw_kwargs = {}
        if raw_table_name:
            raw_kwargs['table_name'] = raw_table_name
        self.raw = RawExtractionSession(endpoint_url, aws_access_key,
                                        aws_secret_key, aws_session_token,
                                        region_name, verify, **raw_kwargs)
        extraction_kwargs = {}
        if extractions_table_name:
            extraction_kwargs['table_name'] = extractions_table_name
        self.extractions = ExtractionSession(endpoint_url, aws_access_key,
                                             aws_secret_key, aws_session_token,
                                             region_name, verify,
                                             **extraction_kwargs)

        references_kwargs = {
            'stored_schema': stored_schema,
            'extracted_schema': extracted_schema
        }
        if references_table_name:
            references_kwargs['table_name'] = references_table_name
        self.references = ReferenceSession(endpoint_url, aws_access_key,
                                           aws_secret_key, aws_session_token,
                                           region_name, verify,
                                           **references_kwargs)


def init_app(app: object) -> None:
    """
    Set default configuration parameters on an application.

    Parameters
    ----------
    app : :class:`flask.Flask` or :class:`celery.Celery`
    """
    config = get_application_config(app)
    config.setdefault('REFLINK_EXTRACTED_SCHEMA',
                      'schema/ExtractedReference.json')
    config.setdefault('REFLINK_STORED_SCHEMA', 'schema/StoredReference.json')
    config.setdefault('DYNAMODB_ENDPOINT',
                      'https://dynamodb.us-east-1.amazonaws.com')
    config.setdefault('AWS_REGION', 'us-east-1')
    config.setdefault('DYNAMODB_VERIFY', 'true')
    config.setdefault('RAW_TABLE_NAME', 'RawExtractions')
    config.setdefault('EXTRACTIONS_TABLE_NAME', 'Extractions')
    config.setdefault('REFERENCES_TABLE_NAME', 'StoredReference')


def get_session(app: object = None) -> DataStoreSession:
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
    g = get_application_global()
    access_key, secret_key, token = None, None, None
    if g is not None and 'credentials' in g and \
            config.get('INSTANCE_CREDENTIALS', 'true') == 'true':
        try:
            access_key, secret_key, token = g.credentials.get_credentials()
        except IOError as e:
            logger.debug('failed to load instance credentials: %s', str(e))
    if access_key is None or secret_key is None:
        access_key = config.get('AWS_ACCESS_KEY_ID', None)
        secret_key = config.get('AWS_SECRET_ACCESS_KEY', None)
        token = config.get('AWS_SESSION_TOKEN', None)
    if not access_key or not secret_key:
        raise RuntimeError('Could not find usable credentials')
    extracted_schema = config.get('REFLINK_EXTRACTED_SCHEMA', None)
    stored_schema = config.get('REFLINK_STORED_SCHEMA', None)
    endpoint_url = config.get('DYNAMODB_ENDPOINT', None)
    region_name = config.get('AWS_REGION', 'us-east-1')
    raw_table = config.get('RAW_TABLE_NAME')
    extractions_table = config.get('EXTRACTIONS_TABLE_NAME')
    references_table = config.get('REFERENCES_TABLE_NAME')
    verify = config.get('DYNAMODB_VERIFY', 'true') == 'true'
    return DataStoreSession(endpoint_url, access_key, secret_key, token,
                            region_name, verify=verify,
                            stored_schema=stored_schema,
                            extracted_schema=extracted_schema,
                            raw_table_name=raw_table,
                            extractions_table_name=extractions_table,
                            references_table_name=references_table)


def current_session():
    """Get/create :class:`.ReferenceStoreSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'data_store' not in g:
        g.data_store = get_session()
    return g.data_store


def store_references(*args, **kwargs):
    """
    Store extracted references for a document.

    See :meth:`.references.ReferenceStoreSession.create`.
    """
    return current_session().references.create(*args, **kwargs)


def get_reference(*args, **kwargs):
    """
    Retrieve metadata for a specific reference in a document.

    See :meth:`.references.ReferenceStoreSession.retrieve`.
    """
    return current_session().references.retrieve(*args, **kwargs)


def get_latest_extraction(*args, **kwargs):
    """
    Retrieve info about the most recent extraction for a document.

    See :meth:`.extraction.ExtractionSession.latest`.
    """
    return current_session().extractions.latest(*args, **kwargs)


def get_latest_extractions(*args, **kwargs):
    """
    Retrieve the most recent extracted references for a document.

    See :meth:`.references.ReferenceStoreSession.retrieve_latest`.
    """
    return current_session().references.retrieve_latest(*args, **kwargs)


def store_raw_extraction(*args, **kwargs):
    """
    Store raw extraction metadata for a single extractor.

    See :meth:`.raw.RawExtractionSession.store_extraction`.
    """
    return current_session().raw.store_extraction(*args, **kwargs)


def get_raw_extraction(*args, **kwargs):
    """
    Retrieve raw extraction metadata for a single extractor.

    See :meth:`.raw.RawExtractionSession.get_extraction`.
    """
    return current_session().raw.get_extraction(*args, **kwargs)


def init_db():
    """Create datastore tables."""
    session = current_session()
    session.raw.create_table()
    session.extractions.create_table()
    session.references.create_table()
