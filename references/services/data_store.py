"""Service layer for reference metadata storage."""

from references import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
# from botocore.errorfactory import ResourceInUseException
import datetime
import json
import jsonschema
import os

from flask import _app_ctx_stack as stack
from base64 import b64encode
from decimal import Decimal
from unidecode import unidecode

from references.types import List
ReferenceData = List[dict]

logger = logging.getLogger(__name__)


class ExtractionSession(object):
    """Commemorates extraction work performed on arXiv documents."""

    table_name = 'Extractions'

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str,
                 verify: bool=True) -> None:
        """Load JSON schema for reference metadata, and set up remote table."""
        self.dynamodb = boto3.resource('dynamodb', verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        self.table = self.dynamodb.Table(self.table_name)

    def create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        try:
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'document', 'KeyType': 'HASH'},
                    {'AttributeName': 'version', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {"AttributeName": 'document', "AttributeType": "S"},
                    {"AttributeName": 'version', "AttributeType": "N"}
                ],
                ProvisionedThroughput={    # TODO: make this configurable.
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            waiter = table.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
        except ClientError:
            logger.debug('Table exists: %s' % self.table_name)
            pass

    def _prepare(self, entry: dict) -> dict:
        extraction = self.hash(entry['document'], entry['version'],
                               entry['created'])
        entry.update({
            'extraction': extraction,
            'version': Decimal(entry['version'])
        })
        return entry

    def hash(self, document_id: str, version: float, created: str) -> str:
        """
        Generate a unique ID for the extraction entry.

        Parameters
        ----------
        document_id : str
        version : float
        created : str

        Returns
        -------
        str
            Base64 encoded hash.
        """
        to_encode = '%s:%s:%s' % (document_id, version, created)
        hash_string = bytes(unidecode(to_encode), encoding='ascii')
        # DynamoDB index keys must be < 1024 bytes.
        return str(b64encode(hash_string), encoding='utf-8')[:100]

    def create(self, document_id: str, version: str, created: str) -> None:
        """
        Create a new extraction entry.

        Parameters
        ----------
        document_id : str
        version : float
        created : str
            ISO-8601 datetime string.

        Raises
        ------
        IOError
        """
        entry = self._prepare({
            'created': created,
            'document': document_id,
            'version': version,
        })
        try:
            self.table.put_item(Item=entry)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e) from e

    def latest(self, document_id: str) -> dict:
        """
        Retrieve the most recent extraction for a document.

        Parameters
        ----------
        document_id : str

        Returns
        -------
        dict
        """
        logger.debug('%s: Get latest extraction' % document_id)
        response = self.table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('document').eq(document_id)
        )
        if len(response['Items']) == 0:
            logger.debug('%s: No extraction available' % document_id)
            return None
        logger.debug('%s: Got extraction' % document_id)
        return response['Items'][0]


class ReferenceStoreSession(object):
    """
    Provide a CRUD interface for the reference datastore.

    Parameters
    ----------
    endpoint_url : str
        If ``None``, uses AWS defaults. Mostly useful for local development
        and testing.
    extracted_schema_path : str
        Location of the JSON schema for reference metadata. If this file does
        not exist, no validation will occur.
    stored_schema_path : str
        Location of the JSON schema for reference metadata. If this file does
        not exist, no validation will occur.
    aws_access_key : str
    aws_secret_key : str
    region_name : str
    """

    def __init__(self, endpoint_url: str, extracted_schema_path: str,
                 stored_schema_path: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str,
                 verify: bool=True) -> None:
        """Load JSON schema for reference metadata, and set up remote table."""
        self.dynamodb = boto3.resource('dynamodb', verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        logger.debug('New ReferenceStoreSession with...')
        logger.debug('verify %s' % verify)
        logger.debug('region_name %s' % region_name)
        logger.debug('endpoint_url %s' % endpoint_url)
        logger.debug('aws_access_key_id %s' % aws_access_key)
        try:
            with open(stored_schema_path) as f:
                self.stored_schema = json.load(f)
            self.table_name = self.stored_schema.get('title',
                                                     'StoredReference')
        except (FileNotFoundError, TypeError):
            logger.info("Could not load schema at %s." % stored_schema_path)
            logger.info("Stored reference validation is disabled")
            self.stored_schema = None
            self.table_name = 'StoredReference'

        try:
            with open(extracted_schema_path) as f:
                self.extracted_schema = json.load(f)
            self.table_name = self.extracted_schema.get('title',
                                                        'ExtractedReference')
        except (FileNotFoundError, TypeError):
            logger.info("Could not load schema at %s" % extracted_schema_path)
            logger.info("Extracted reference validation is disabled")
            self.extracted_schema = None
            # self.table_name = 'ExtractedReference'
        self.table = self.dynamodb.Table(self.table_name)
        self.extractions = ExtractionSession(endpoint_url, aws_access_key,
                                             aws_secret_key, region_name,
                                             verify=verify)

    def create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        try:
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'document_extraction', 'KeyType': 'HASH'},
                    {'AttributeName': 'order', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {"AttributeName": 'document_extraction', "AttributeType": "S"},
                    {"AttributeName": 'document', "AttributeType": "S"},
                    # {"AttributeName": 'extraction', "AttributeType": "S"},
                    {"AttributeName": 'identifier', "AttributeType": "S"},
                    {"AttributeName": 'order', "AttributeType": "N"},
                    # {"AttributeName": 'version', "AttributeType": "N"}
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'DocumentVersionIndex',
                        'KeySchema': [
                            {'AttributeName': 'document', 'KeyType': 'HASH'},
                            {'AttributeName': 'identifier', 'KeyType': 'RANGE'}
                        ],
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        },
                        'Projection': {
                            "ProjectionType": 'ALL'
                        }
                    },
                ],
                ProvisionedThroughput={    # TODO: make this configurable.
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            waiter = table.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
        except ClientError:
            logger.debug('Table exists: %s' % self.table_name)
            pass

        self.extractions.create_table()

    def validate_extracted(self, reference: dict,
                           raise_on_invalid: bool = True) -> bool:
        """
        Validate reference data against the configured JSON schema.

        Parameters
        ----------
        data : list
        raise_on_invalid : bool
            If True, a ValueError will be raised if invalid. Otherwise, just
            returns False.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            Raised when the data in ``references`` is invalid.
        """
        if self.extracted_schema is None:
            logger.debug("No schema available; skipping validation.")
            return True

        # for reference in data:
        try:
            jsonschema.validate(reference, self.extracted_schema)
        except jsonschema.ValidationError as e:
            logger.error("Invalid data: %s" % e)
            if raise_on_invalid:
                raise ValueError('%s' % e) from e
            return False
        return True

    def validate_stored(self, reference: dict,
                        raise_on_invalid: bool = True) -> bool:
        """
        Validate reference data against the configured JSON schema.

        Parameters
        ----------
        data : list
        raise_on_invalid : bool
            If True, a ValueError will be raised if invalid. Otherwise, just
            returns False.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            Raised when the data in ``references`` is invalid.
        """
        if self.stored_schema is None:
            logger.debug("No schema available; skipping validation.")
            return True

        # for reference in data:
        try:
            jsonschema.validate(reference, self.stored_schema)
        except jsonschema.ValidationError as e:
            logger.error("Invalid data: %s" % e)
            if raise_on_invalid:
                raise ValueError('%s' % e) from e
            return False
        return True

    def hash(self, document_id: str, raw: str, version: str):
        """
        Generate a unique hash for an extracted reference.

        Parameters
        ----------
        document_id : str
        raw : str
            Raw reference string.
        version: str

        Returns
        -------
        bytes
            Base64-encoded identifier.
        """
        to_encode = '%s:%s:%s' % (document_id, raw, version)
        hash_string = bytes(unidecode(to_encode), encoding='ascii')
        return str(b64encode(hash_string), encoding='utf-8')[:100]

    @staticmethod
    def _clean(reference: dict) -> dict:
        """
        Remove empty values.

        Parameters
        ----------
        reference : dict

        Returns
        -------
        dict
        """
        if reference is None:
            return

        def _inner_clean(datum):
            if isinstance(datum, dict):
                return {k: _inner_clean(v) for k, v in datum.items() if v}
            elif isinstance(datum, list):
                return [_inner_clean(v) for v in datum if v]
            return datum

        return {field: _inner_clean(value) for field, value
                in reference.items() if value}

    def create(self, document_id: str, references: ReferenceData,
               version: str) -> None:
        """
        Insert a new reference data set into the data store.

        Parameters
        ----------
        document_id : str
            arXiv identifier for a document.
        references : list
            A list of references (dicts) extracted from a document.
        version : str
            The application version for this extraction.

        Returns
        -------
        str
            Extraction identifier.
        list
            Reference data, updated with identifiers.

        Raises
        ------
        ValueError
            Raised when the data in ``references`` is invalid.
        IOError
            Raised when the data was not successfully written to the database.
        """
        created = datetime.datetime.now().isoformat()
        extraction = self.extractions.hash(document_id, version, created)
        document_extraction = '%s#%s' % (document_id, extraction)
        stored_references = []
        try:
            with self.table.batch_writer() as batch:
                for order, reference in enumerate(references):
                    reference = self._clean(reference)
                    self.validate_extracted(reference)

                    # Generate a unique identifier based on the document, raw
                    #  reference line, and arxiv-references software version.
                    identifier = self.hash(document_id, reference.get('raw'),
                                           version)

                    # References are considered citations by default.
                    reftype = reference.get('reftype', None)
                    if not reftype:
                        reftype = 'citation'

                    reference.update({
                        'document': document_id,
                        'document_extraction': document_extraction,
                        'created': created,
                        'version': Decimal(version),
                        'identifier': identifier,
                        'order': order
                    })

                    self.validate_stored(reference)
                    stored_references.append(reference)
                    batch.put_item(Item=reference)
        except ClientError as e:
            raise IOError('Failed to create: %s; %s' % (e, reference)) from e

        self.extractions.create(document_id, version, created)
        return extraction, stored_references

    def retrieve(self, document_id: str, identifier: str) -> dict:
        """
        Retrieve metadata for a specific reference in a document.

        Parameters
        ----------
        document_id : str
        identifier: str

        Returns
        -------
        dict
        """
        logger.debug('%s: Retrieve reference %s' % (document_id, identifier))
        expression = Key('document').eq(document_id) \
            & Key('identifier').eq(identifier)

        response = self.table.query(
            IndexName='DocumentVersionIndex',
            KeyConditionExpression=expression,
            Limit=1
        )
        if len(response['Items']) == 0:
            logger.debug('%s: not found %s' % (document_id, identifier))
            return None
        logger.debug('%s: found %s' % (document_id, identifier))
        return response['Items'][0]

    def retrieve_latest(self, document_id: str,
                        reftype: str='__all__') -> dict:
        """
        Retrieve the most recent extracted references for a document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.5678``.
        reftype : str
            If provided, returns only references with a specific ``reftype``.

        Returns
        -------
        list or None
        """
        logger.debug('%s: Retrieve latest references' % document_id)
        latest = self.extractions.latest(document_id)
        if latest is None:
            return None    # No extractions for this document.
        return self.retrieve_all(document_id, latest.get('extraction'),
                                 reftype=reftype)

    def retrieve_all(self, document_id: str, extraction: str = None,
                     reftype: str='__all__') -> dict:
        """
        Retrieve reference metadata for an arXiv document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.5678``.
        extraction : str
            Extraction version.
        reftype : str
            If provided, returns only references with a specific ``reftype``.

        Returns
        -------
        list or None

        Raises
        ------
        IOError
            Raised when we are unable to read from the DynamoDB database, or
            when the database returns a malformed response.
        """
        logger.debug('%s: Retrieve extraction %s' % (document_id, extraction))
        document_extraction = '%s#%s' % (document_id, extraction)
        expression = Key('document_extraction').eq(document_extraction)
        try:
            response = self.table.query(
                # IndexName='DocumentOrderIndex',
                Select='ALL_ATTRIBUTES',
                KeyConditionExpression=expression
            )
        except ClientError as e:
            raise IOError('Failed to read: %s' % e) from e

        if 'Items' not in response or len(response['Items']) == 0:
            logger.debug('%s: not found %s' % (document_id, extraction))
            return None    # No such record.

        references = response['Items']
        logger.debug('%s: found %s' % (document_id, extraction))
        if reftype != '__all__':
            return [ref for ref in references if ref['reftype'] == reftype]
        return references


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
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'asdf1234')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'fdsa5678')
        app.config.setdefault('AWS_REGION', 'us-east-1')
        app.config.setdefault('DYNAMODB_VERIFY', 'true')

    def get_session(self) -> ReferenceStoreSession:
        """
        Initialize a session with the data store.

        Returns
        -------
        :class:`.ReferenceStoreSession`
        """
        try:
            extracted_schema_path = self.app.config['REFLINK_EXTRACTED_SCHEMA']
            stored_schema_path = self.app.config['REFLINK_STORED_SCHEMA']
            endpoint_url = self.app.config['DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['AWS_REGION']
            verify = self.app.config['DYNAMODB_VERIFY'] == 'true'
        except (RuntimeError, AttributeError) as e:   # No application context.
            extracted_schema_path = os.environ.get('REFLINK_EXTRACTED_SCHEMA',
                                                   None)
            stored_schema_path = os.environ.get('REFLINK_STORED_SCHEMA', None)
            endpoint_url = os.environ.get('DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
            region_name = os.environ.get('AWS_REGION', 'us-east-1')
            verify = os.environ.get('DYNAMODB_VERIFY', 'true') == 'true'
        return ReferenceStoreSession(endpoint_url, extracted_schema_path,
                                     stored_schema_path, aws_access_key,
                                     aws_secret_key, region_name,
                                     verify=verify)

    @property
    def session(self) -> ReferenceStoreSession:
        """The current data store session."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'data_store'):
                ctx.data_store = self.get_session()
            return ctx.data_store
        return self.get_session()     # No application context.


referencesStore = DataStore()
