"""
Service layer for reference metadata storage and retrieval.

All of the work is done by the submodules:

.. autodoc::
   :members:


"""

from references import logging
import os

# See http://flask.pocoo.org/docs/0.12/extensiondev/
from flask import _app_ctx_stack as stack
from flask import current_app
from references.types import List
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


class DataStore(object):
    """Data store service integration from references Flask application."""

    def __init__(self, app: object=None) -> None:
        """
        Set the application if available.

        Parameters
        ----------
        app : :class:`flask.Flask` or :class:`celery.Celery`
        """
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app: object) -> None:
        """
        Set default configuration parameters on an application.

        Parameters
        ----------
        app : :class:`flask.Flask` or :class:`celery.Celery`
        """
        app.config.setdefault('REFLINK_EXTRACTED_SCHEMA', None)
        app.config.setdefault('REFLINK_STORED_SCHEMA', None)
        app.config.setdefault('DYNAMODB_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'null')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'null')
        app.config.setdefault('AWS_SESSION_TOKEN', None)
        app.config.setdefault('AWS_REGION', 'us-east-1')
        app.config.setdefault('DYNAMODB_VERIFY', 'true')
        app.config.setdefault('RAW_TABLE_NAME', 'RawExtractions')
        app.config.setdefault('EXTRACTIONS_TABLE_NAME', 'Extractions')
        app.config.setdefault('REFERENCES_TABLE_NAME', 'StoredReference')

    def get_app(self):
        """
        Get the current application, if available.

        Returns
        -------
        :class:`flask.Flask`

        Raises
        ------
        RuntimeError
            Raised when no application is available.
        """
        if current_app:
            return current_app._get_current_object()

        if self.app is not None:
            return self.app

    def get_session(self, app=None) -> DataStoreSession:
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
        if app is None:
            app = self.get_app()
        logger.debug('get_session')
        logger.debug('app is %s' % str(app))

        try:
            extracted_schema = app.config['REFLINK_EXTRACTED_SCHEMA']
            stored_schema = app.config['REFLINK_STORED_SCHEMA']
            endpoint_url = app.config['DYNAMODB_ENDPOINT']
            aws_access_key = app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = app.config['AWS_SECRET_ACCESS_KEY']
            aws_session_token = app.config['AWS_SESSION_TOKEN']
            region_name = app.config['AWS_REGION']
            raw_table = app.config['RAW_TABLE_NAME']
            extractions_table = app.config['EXTRACTIONS_TABLE_NAME']
            references_table = app.config['REFERENCES_TABLE_NAME']
            verify = app.config['DYNAMODB_VERIFY'] == 'true'
            logger.debug('got config from app')
        except (RuntimeError, AttributeError) as e:   # No application context.
            extracted_schema = os.environ.get('REFLINK_EXTRACTED_SCHEMA', None)
            stored_schema = os.environ.get('REFLINK_STORED_SCHEMA', None)
            endpoint_url = os.environ.get('DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
            aws_session_token = os.environ.get('AWS_SESSION_TOKEN', None)
            region_name = os.environ.get('AWS_REGION', 'us-east-1')
            raw_table = os.environ.get('RAW_TABLE_NAME')
            extractions_table = os.environ.get('EXTRACTIONS_TABLE_NAME')
            references_table = os.environ.get('REFERENCES_TABLE_NAME')
            verify = os.environ.get('DYNAMODB_VERIFY', 'true') == 'true'
            logger.debug('got config from environ')
        return DataStoreSession(endpoint_url, aws_access_key, aws_secret_key,
                                aws_session_token, region_name, verify=verify,
                                stored_schema=stored_schema,
                                extracted_schema=extracted_schema,
                                raw_table_name=raw_table,
                                extractions_table_name=extractions_table,
                                references_table_name=references_table)

    @property
    def session(self) -> DataStoreSession:
        """The current data store session."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'data_store'):
                ctx.data_store = self.get_session()
            return ctx.data_store
        return self.get_session()     # No application context.


referencesStore = DataStore()


def store_references(*args, **kwargs):
    """
    Store extracted references for a document.

    See :meth:`.references.ReferenceStoreSession.create`.
    """
    return referencesStore.session.references.create(*args, **kwargs)


def get_reference(*args, **kwargs):
    """
    Retrieve metadata for a specific reference in a document.

    See :meth:`.references.ReferenceStoreSession.retrieve`.
    """
    return referencesStore.session.references.retrieve(*args, **kwargs)


def get_latest_extraction(*args, **kwargs):
    """
    Retrieve info about the most recent extraction for a document.

    See :meth:`.extraction.ExtractionSession.latest`.
    """
    return referencesStore.session.extractions.latest(*args, **kwargs)


def get_latest_extractions(*args, **kwargs):
    """
    Retrieve the most recent extracted references for a document.

    See :meth:`.references.ReferenceStoreSession.retrieve_latest`.
    """
    return referencesStore.session.references.retrieve_latest(*args, **kwargs)


def store_raw_extraction(*args, **kwargs):
    """
    Store raw extraction metadata for a single extractor.

    See :meth:`.raw.RawExtractionSession.store_extraction`.
    """
    return referencesStore.session.raw.store_extraction(*args, **kwargs)


def get_raw_extraction(*args, **kwargs):
    """
    Retrieve raw extraction metadata for a single extractor.

    See :meth:`.raw.RawExtractionSession.get_extraction`.
    """
    return referencesStore.session.raw.get_extraction(*args, **kwargs)


def init_db():
    """Create datastore tables."""
    referencesStore.session.raw.create_table()
    referencesStore.session.extractions.create_table()
    referencesStore.session.references.create_table()
