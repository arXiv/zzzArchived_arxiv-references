"""Stores entry about an extraction session."""

from base64 import b64encode
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from unidecode import unidecode

from references import logging
from typing import List

ReferenceData = List[dict]

logger = logging.getLogger(__name__)


class ExtractionSession(object):
    """Commemorates extraction work performed on arXiv documents."""

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, aws_session_token: str,
                 region_name: str, verify: bool=True,
                 table_name: str = 'Extractions') -> None:
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
            logger.debug('Table exists: %s', self.table_name)

    def _prepare(self, entry: dict) -> dict:
        extraction = self.hash(entry['document'], entry['version'],
                               entry['created'])
        entry.update({
            'extraction': extraction,
            'version': Decimal(entry['version'])
        })
        return entry

    @staticmethod
    def hash(document_id: str, version: str, created: str) -> str:
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

    def create(self, document_id: str, version: str, created: str,
               score: float = 1.0, extractors: list = []) -> None:
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
            'score': Decimal(str(score)),
        })
        if extractors:
            entry['extractors'] = extractors
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
        logger.debug('%s: Get latest extraction', document_id)
        response = self.table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('document').eq(document_id)
        )
        if len(response['Items']) == 0:
            logger.debug('%s: No extraction available', document_id)
            return None
        logger.debug('%s: Got extraction', document_id)
        return response['Items'][0]
