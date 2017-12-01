"""Storage for extracted reference metadata."""

import datetime
import json
from base64 import b64encode
from decimal import Decimal
import os

import jsonschema
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from unidecode import unidecode

from references import logging
from typing import List, Tuple

from .extractions import ExtractionSession
from .util import clean

ReferenceData = List[dict]

logger = logging.getLogger(__name__)


class ReferenceSession(object):
    """
    Provide a CRUD interface for the reference datastore.

    Parameters
    ----------
    endpoint_url : str
        If ``None``, uses AWS defaults. Mostly useful for local development
        and testing.
    extracted_schema : str
        Location of the JSON schema for reference metadata. If this file does
        not exist, no validation will occur.
    stored_schema : str
        Location of the JSON schema for reference metadata. If this file does
        not exist, no validation will occur.
    aws_access_key : str
    aws_secret_key : str
    region_name : str
    """

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, aws_session_token: str, region_name: str,
                 verify: bool=True, extracted_schema: str = None,
                 stored_schema: str = None,
                 table_name: str = 'StoredReference') -> None:
        """Load JSON schema for reference metadata, and set up remote table."""
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key,
                                       aws_session_token=aws_session_token)
        self.table = self.dynamodb.Table(self.table_name)
        self.extracted_schema = None
        self.stored_schema = None
        if extracted_schema and os.path.exists(extracted_schema):
            with open(extracted_schema) as f:
                self.extracted_schema = json.load(f)
        if stored_schema and os.path.exists(stored_schema):
            with open(stored_schema) as f:
                self.stored_schema = json.load(f)

        self.extractions = ExtractionSession(endpoint_url, aws_access_key,
                                             aws_secret_key, aws_session_token,
                                             region_name, verify=verify)

    def create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        try:
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'document_extraction',
                     'KeyType': 'HASH'},
                    {'AttributeName': 'order', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {"AttributeName": 'document_extraction',
                     "AttributeType": "S"},
                    {"AttributeName": 'document', "AttributeType": "S"},
                    {"AttributeName": 'identifier', "AttributeType": "S"},
                    {"AttributeName": 'order', "AttributeType": "N"},
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
            logger.debug('Table exists: %s', self.table_name)

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
            # logger.debug("No schema available; skipping validation.")
            return True

        # for reference in data:
        try:
            jsonschema.validate(reference, self.extracted_schema)
        except jsonschema.ValidationError as e:
            logger.error("Invalid data: %s", e)
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
            # logger.debug("No schema available; skipping validation.")
            return True

        # for reference in data:
        try:
            jsonschema.validate(reference, self.stored_schema)
        except jsonschema.ValidationError as e:
            logger.error("Invalid data: %s", e)
            if raise_on_invalid:
                raise ValueError('%s' % e) from e
            return False
        return True

    @staticmethod
    def hash(document_id: str, raw: str, version: str):
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

    def create(self, document_id: str, references: ReferenceData,
               version: str, score: float=1.0,
               extractors: list = []) -> Tuple[str, list]:
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
                    reference = clean(reference)
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
                        'order': order + 1
                    })

                    self.validate_stored(reference)
                    stored_references.append(reference)
                    batch.put_item(Item=reference)
        except ClientError as e:
            raise IOError('Failed to create: %s; %s' % (e, reference)) from e

        self.extractions.create(document_id, version, created, score=score,
                                extractors=extractors)
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
        logger.debug('%s: Retrieve reference %s', document_id, identifier)
        expression = Key('document').eq(document_id) \
            & Key('identifier').eq(identifier)

        response = self.table.query(
            IndexName='DocumentVersionIndex',
            KeyConditionExpression=expression,
            Limit=1
        )
        if len(response['Items']) == 0:
            logger.debug('%s: not found %s', document_id, identifier)
            return None
        logger.debug('%s: found %s', document_id, identifier)
        return response['Items'][0]

    def retrieve_latest(self, document_id: str,
                        reftype: str = '__all__') -> dict:
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
        logger.debug('%s: Retrieve latest references', document_id)
        latest = self.extractions.latest(document_id)
        if latest is None:
            return None    # No extractions for this document.
        references = self.retrieve_all(document_id, latest.get('extraction'),
                                       reftype=reftype)
        latest['references'] = references
        return latest

    def retrieve_all(self, document_id: str, extraction: str = None,
                     reftype: str = '__all__') -> list:
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
        list

        Raises
        ------
        IOError
            Raised when we are unable to read from the DynamoDB database, or
            when the database returns a malformed response.
        """
        logger.debug('%s: Retrieve extraction %s', document_id, extraction)
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
            logger.debug('%s: not found %s', document_id, extraction)
            return None    # No such record.

        references = response['Items']
        logger.debug('%s: found %s', document_id, extraction)
        if reftype != '__all__':
            return [ref for ref in references if ref['reftype'] == reftype]
        return references
