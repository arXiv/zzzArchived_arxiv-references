"""The request table tracks work on arXiv documents."""

from reflink import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime
import os
from flask import _app_ctx_stack as stack

logger = logging.getLogger(__name__)


class ExtractionEventSession(object):
    """Commemorates events for exaction on arXiv documents."""

    table_name = 'ExtractionEvents'

    REQUESTED = 'REQU'
    FAILED = 'FAIL'
    COMPLETED = 'COMP'
    STATES = (REQUESTED, FAILED, COMPLETED)

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str) -> None:
        """Set up remote table."""
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        try:
            self._create_table()
        except ClientError as e:
            # logger.info('Table already exists: %s' % self.table_name)
            pass
        self.table = self.dynamodb.Table(self.table_name)

    def _create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': 'document', 'KeyType': 'HASH'},
                {'AttributeName': 'created', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {"AttributeName": 'document', "AttributeType": "S"},
                {"AttributeName": 'created', "AttributeType": "S"}
            ],
            ProvisionedThroughput={    # TODO: make this configurable.
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = table.meta.client.get_waiter('table_exists')
        waiter.wait(TableName=self.table_name)

    def create(self, document_id: str, state: str=REQUESTED, **extra) -> None:
        """
        Create a new extraction event entry.

        Parameters
        ----------
        document_id : str

        Raises
        ------
        IOError
        """
        if state not in ExtractionEventSession.STATES:
            raise ValueError('Invalid state: %s' % state)

        entry = dict(extra)
        entry.update({
            'created': datetime.now().isoformat(),
            'document': document_id,
            'state': state,
        })
        try:
            self.table.put_item(Item=entry)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e) from e

    def latest(self, document_id: str) -> dict:
        """
        Retrieve the most recent event for a document.

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


class ExtractionEvents(object):
    """Extraction event store service integration."""

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('REFLINK_DYNAMODB_ENDPOINT', None)
        app.config.setdefault('REFLINK_AWS_ACCESS_KEY', 'asdf1234')
        app.config.setdefault('REFLINK_AWS_SECRET_KEY', 'fdsa5678')
        app.config.setdefault('REFLINK_AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['REFLINK_DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['REFLINK_AWS_ACCESS_KEY']
            aws_secret_key = self.app.config['REFLINK_AWS_SECRET_KEY']
            region_name = self.app.config['REFLINK_AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('REFLINK_DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('REFLINK_AWS_ACCESS_KEY', 'asdf')
            aws_secret_key = os.environ.get('REFLINK_AWS_SECRET_KEY', 'fdsa')
            region_name = os.environ.get('REFLINK_AWS_REGION', 'us-east-1')
        return ExtractionEventSession(endpoint_url, aws_access_key,
                                      aws_secret_key, region_name)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'extraction_events'):
                ctx.extraction_events = self.get_session()
            return ctx.extraction_events
        return self.get_session()     # No application context.
