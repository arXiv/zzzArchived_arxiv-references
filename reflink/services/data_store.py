import boto3
from botocore.exceptions import ClientError
import datetime
import json
import jsonschema
import logging
import os

from typing import List

Data = List[dict]


class ReferenceStoreSession(object):
    def __init__(self, endpoint_url: str, schema_path: str,
                 aws_access_key: str, aws_secret_key: str) -> None:
        self.dynamodb = boto3.resource('dynamodb',
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        with open(schema_path) as f:
            self.schema = json.load(f)
        self.table_name = self.schema.get('title', 'ReferenceSet')

        try:
            self._create_table()
        except Exception as e:    # The table already exists.
            pass
        self.table = self.dynamodb.Table(self.table_name)


    def _create_table(self) -> None:
        """
        Set up a new table in DynamoDB. Blocks until table is available.
        """
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'document',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": 'document',
                    "AttributeType": "S"
                }
            ],
            ProvisionedThroughput={    # TODO: make this configurable.
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)


    def validate(self, data: dict, raise_on_invalid: bool = True) -> bool:
        """
        Validate reference data against the configured JSON schema.

        Parameters
        ----------
        data : dict
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
        try:
            ack = jsonschema.validate(data, self.schema)
        except jsonschema.ValidationError as E:
            if raise_on_invalid:
                raise ValueError(E.message)
            return False
        return True

    def _prepare(self, document_id: str, references: Data) -> dict:
        now = datetime.datetime.now().isoformat()
        data = {
            'document': document_id,
            'references': references,
            'created': now,
            'updated': now
        }
        return data

    def create(self, document_id: str, references: Data) -> None:
        """
        Insert a new reference data set into the data store.

        Parameters
        ----------
        document_id : str
            arXiv identifier for a document.
        references : list
            A list of references (dicts) extracted from a document.

        Raises
        ------
        ValueError
            Raised when the data in ``references`` is invalid.
        IOError
            Raised when the data was not successfully written to the database.
        """

        data = self._prepare(document_id, references)
        self.validate(data)    # Allow ValueError to percolate up.

        try:
            self.table.put_item(Item=data)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e)

    def retrieve(self, document_id: str) -> dict:
        """
        Retrieve reference data for an arXiv document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.5678``.

        Returns
        -------
        dict

        Raises
        ------
        IOError
            Raised when we are unable to read from the DynamoDB database, or
            when the database returns a malformed response.
        """
        try:
            response = self.table.get_item(Key={'document': document_id})
        except ClientError as e:
            raise IOError('Failed to read: %s' % e)

        if 'Item' not in response:   # No such record.
            return None

        if 'references' not in response['Item']:
            raise IOError('Bad response from database: %s' % e)

        return response['Item']['references']



def get_session() -> ReferenceStoreSession:
    """
    Get a new database session.

    Returns
    -------
    :class:`.ReferenceStoreSession`
    """
    schema_path = os.environ.get('REFLINK_SCHEMA', None)
    endpoint_url = os.environ.get('REFLINK_DYNAMODB_ENDPOINT', None)
    aws_access_key = os.environ.get('REFLINK_AWS_ACCESS_KEY', 'asdf1234')
    aws_secret_key = os.environ.get('REFLINK_AWS_SECRET_KEY', 'fdsa5678')
    return ReferenceStoreSession(endpoint_url, schema_path, aws_access_key,
                                 aws_secret_key)
