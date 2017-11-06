"""Stores raw extraction metadata from individual extractors."""

import os
import json
import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import jsonschema
from references import logging
from .util import clean

logger = logging.getLogger(__name__)


class RawExtractionSession(object):
    """Commemorates output of individual extractors, prior to arbitration."""

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, aws_session_token: str,
                 region_name: str, verify: bool=True,
                 table_name: str='RawExtractions') -> None:
        """Load JSON schema for reference metadata, and set up remote table."""
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key,
                                       aws_session_token=aws_session_token)
        self.table = self.dynamodb.Table(self.table_name)

    def create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        try:
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'document_extractor', 'KeyType': 'HASH'},
                    {'AttributeName': 'created', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {"AttributeName": 'document_extractor',
                     "AttributeType": "S"},
                    {"AttributeName": 'extractor', "AttributeType": "S"},
                    {"AttributeName": 'created', "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'ExtractorIndex',
                        'KeySchema': [
                            {'AttributeName': 'extractor', 'KeyType': 'HASH'},
                        ],
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        },
                        'Projection': {
                            "ProjectionType": 'ALL'
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            waiter = table.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
        except ClientError:
            logger.debug('Table exists: %s', self.table_name)

    @staticmethod
    def hash(document_id: str, extractor: str) -> str:
        """Generate the hash key for the primary index."""
        return "%s:%s" % (document_id, extractor)

    def store_extraction(self, document_id: str, extractor: str,
                         references: list) -> None:
        """
        Store raw reference metadata for a single extractor.

        Parameters
        ----------
        document_id : str
            arXiv paper ID or submission ID, preferably including the
            version. E.g. ``"1606.00123v3"``.
        extractor : str
            Name of the reference extractor (e.g. ``"grobid"``).
        references : list
            Extraction metadata. Should be a list of ``dict``, each of
            which represents a single cited reference.

        Raises
        ------
        ValueError
            Invalid value for one or more parameters.
        """
        if not document_id or not isinstance(document_id, str):
            raise ValueError('Not a valid document ID, expected str')
        if not extractor or not isinstance(extractor, str):
            raise ValueError('Not a valid extractor name, expected str')
        if not references or not isinstance(references, list):
            raise ValueError('Invalid value for references, expected list')

        entry = {
            'document_extractor': self.hash(document_id, extractor),
            'document': document_id,
            'created': datetime.datetime.now().isoformat(),
            'extractor': extractor,
            'references': [clean(ref) for ref in references if ref]
        }
        try:
            self.table.put_item(Item=entry)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e) from e

    def get_extraction(self, document_id: str, extractor: str) -> dict:
        """
        Retrieve extraction metadata for a specific extractor.

        Parameters
        ----------
        document_id : str
            arXiv paper ID or submission ID, preferably including the
            version. E.g. ``"1606.00123v3"``.
        extractor : str
            Name of the reference extractor (e.g. ``"grobid"``).

        Returns
        -------
        dict
            Includes metadata about the extraction, and the references
            themselves.
        """
        logger.debug('%s: get latest from %s', document_id, extractor)
        key = self.hash(document_id, extractor)
        try:
            response = self.table.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('document_extractor').eq(key)
            )
        except ClientError as e:
            raise IOError('Failed to query DynamoDB table %s: %s' %
                          (self.table_name, e)) from e
        if len(response['Items']) == 0:
            logger.debug('%s: no extraction from %s', document_id, extractor)
            return None
        logger.debug('%s: got extraction from %s', document_id, extractor)
        return response['Items'][0]
