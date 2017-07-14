"""Service layer for reference metadata storage."""

import logging
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
# from botocore.errorfactory import ResourceInUseException
import datetime
import json
import jsonschema
import os
from base64 import b64encode
from decimal import Decimal

from typing import List
ReferenceData = List[dict]

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
logger = logging.getLogger(__name__)




class ExtractionSession(object):
    """Commemorates extraction work performed on arXiv documents."""

    table_name = 'Extractions'

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str) -> None:
        """Load JSON schema for reference metadata, and set up remote table."""
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        try:
            self._create_table()
        except ClientError as e:
            logger.info('Table already exists: %s' % self.table_name)
        self.table = self.dynamodb.Table(self.table_name)

    def _create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
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
        hash_string = bytes(to_encode, encoding='ascii')
        return str(b64encode(hash_string), encoding='utf-8')

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
        response = self.table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('document').eq(document_id)
        )
        if len(response['Items']) == 0:
            return None
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
                 aws_secret_key: str, region_name: str) -> None:
        """Load JSON schema for reference metadata, and set up remote table."""
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)

        try:
            with open(stored_schema_path) as f:
                self.stored_schema = json.load(f)
            self.table_name = self.stored_schema.get('title',
                                                     'StoredReference')
        except (FileNotFoundError, TypeError):
            logger.error("Could not load schema at %s." % stored_schema_path)
            logger.info("Stored reference validation is disabled")
            self.stored_schema = None
            self.table_name = 'StoredReference'

        try:
            with open(extracted_schema_path) as f:
                self.extracted_schema = json.load(f)
            self.table_name = self.extracted_schema.get('title',
                                                        'ExtractedReference')
        except (FileNotFoundError, TypeError):
            logger.error("Could not load schema at %s" % extracted_schema_path)
            logger.info("Extracted reference validation is disabled")
            self.extracted_schema = None
            # self.table_name = 'ExtractedReference'

        try:
            self._create_table()
        except ClientError as e:
            logger.info('Table already exists: %s' % self.table_name)
        self.table = self.dynamodb.Table(self.table_name)

        self.extractions = ExtractionSession(endpoint_url, aws_access_key,
                                             aws_secret_key, region_name)

    def _create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
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
            logger.info("No schema available; skipping validation.")
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
            logger.info("No schema available; skipping validation.")
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
        hash_string = bytes(to_encode, encoding='ascii')
        return str(b64encode(hash_string), encoding='utf-8')

    def _clean(self, reference: dict) -> dict:
        """
        Remove empty values.

        Parameters
        ----------
        reference : dict

        Returns
        -------
        dict
        """
        def _inner_clean(datum):
            return {k: v for k, v in datum if v}

        return {k: v if v and k not in ['authors', 'identifiers']
                else [_inner_clean(datum) for datum in v]
                for k, v in reference.items()}


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

        try:
            with self.table.batch_writer() as batch:
                for order, reference in enumerate(references):
                    reference = self._clean(reference)
                    self.validate_extracted(reference)
                    identifier = self.hash(document_id, reference['raw'],
                                           version)
                    reference.update({
                        'document': document_id,
                        'document_extraction': document_extraction,
                        'created': created,
                        'version': Decimal(version),
                        'identifier': identifier,
                        'order': order
                    })
                    self.validate_stored(reference)
                    # self.table.put_item(Item=reference)
                    batch.put_item(Item=reference)
        except ClientError as e:
            raise IOError('Failed to create: %s; %s' % (e, reference)) from e

        self.extractions.create(document_id, version, created)
        return extraction, references

    def retrieve(self, document_id: str, identifier: str) -> dict:
        """
        """
        expression = Key('document').eq(document_id) \
            & Key('identifier').eq(identifier)

        response = self.table.query(
            IndexName='DocumentVersionIndex',
            KeyConditionExpression=expression,
            Limit=1
        )
        if len(response['Items']) == 0:
            msg = 'No such reference %s for document %s' %\
                (identifier, document_id)
            raise IOError(msg)
        return response['Items'][0]

    def retrieve_latest(self, document_id: str) -> dict:
        """
        Retrieve the most recent extracted references for a document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.5678``.

        Returns
        -------
        list or None
        """
        latest = self.extractions.latest(document_id)
        if latest is None:
            return None    # No extractions for this document.
        return self.retrieve_all(document_id, latest.get('extraction'))

    def retrieve_all(self, document_id: str, extraction: str = None) -> dict:
        """
        Retrieve reference data for an arXiv document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.5678``.
        extraction : str
            Extraction version.

        Returns
        -------
        list or None

        Raises
        ------
        IOError
            Raised when we are unable to read from the DynamoDB database, or
            when the database returns a malformed response.
        """
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
            return None    # No such record.

        return response['Items']


def get_session() -> ReferenceStoreSession:
    """
    Get a new database session.

    Returns
    -------
    :class:`.ReferenceStoreSession`
    """
    extracted_schema_path = os.environ.get('REFLINK_EXTRACTED_SCHEMA', None)
    stored_schema_path = os.environ.get('REFLINK_STORED_SCHEMA', None)
    endpoint_url = os.environ.get('REFLINK_DYNAMODB_ENDPOINT', None)
    aws_access_key = os.environ.get('REFLINK_AWS_ACCESS_KEY', 'asdf1234')
    aws_secret_key = os.environ.get('REFLINK_AWS_SECRET_KEY', 'fdsa5678')
    region_name = os.environ.get('REFLINK_AWS_REGION', 'us-east-1')
    return ReferenceStoreSession(endpoint_url,
                                 extracted_schema_path=extracted_schema_path,
                                 stored_schema_path=stored_schema_path,
                                 aws_access_key=aws_access_key,
                                 aws_secret_key=aws_secret_key,
                                 region_name=region_name)
