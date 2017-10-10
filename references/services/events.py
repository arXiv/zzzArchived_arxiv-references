"""The request table tracks work on arXiv documents."""

from references import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime
import os

# See http://flask.pocoo.org/docs/0.12/extensiondev/
from flask import _app_ctx_stack as stack

logger = logging.getLogger(__name__)


class ExtractionEventSession(object):
    """Commemorates events for exaction on arXiv documents."""

    table_name = 'ReferenceExtractionProgress'

    REQUESTED = 'REQU'
    FAILED = 'FAIL'
    COMPLETED = 'COMP'
    STATES = (REQUESTED, FAILED, COMPLETED)

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, aws_session_token: str, region_name: str,
                 version: str, verify: bool=True) -> None:
        """Set up remote table."""
        logger.info('Connect to dynamodb on %s' % endpoint_url)
        self.dynamodb = boto3.resource('dynamodb',
                                       verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key,
                                       aws_session_token=aws_session_token)
        self.version = version
        self.table = self.dynamodb.Table(self.table_name)

    def create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': 'document', 'KeyType': 'HASH'},
                {'AttributeName': 'version', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {"AttributeName": 'document', "AttributeType": "S"},
                {'AttributeName': 'version', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={    # TODO: make this configurable.
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = table.meta.client.get_waiter('table_exists')
        waiter.wait(TableName=self.table_name)

    def update_or_create(self, sequence_id: int, state: str=REQUESTED,
                         document_id: str=None, **extra) -> None:
        """
        Create a new extraction event entry.

        Parameters
        ----------
        sequence_id : int
        state : str
        document_id : str

        Raises
        ------
        IOError
        """
        if state not in ExtractionEventSession.STATES:
            raise ValueError('Invalid state: %s' % state)

        _attributeValues = {}
        _attributeValues.update({
            ':seq': str(sequence_id),
            ':updated': datetime.now().isoformat(),
            ':state': state,
        })
        _attributeNames = {
            '#seq': 'sequence',
            '#upd': 'updated',
            '#st': 'state'
        }
        _key = {'document': document_id, 'version': self.version}
        _updateExpressionParts = ['#seq=:seq', '#upd=:updated', '#st=:state']
        if 'extraction' in extra:
            _attributeValues[':extr'] = extra.get('extraction')
            _attributeNames['#extr'] = 'extraction'
            _updateExpressionParts.append('#extr=:extr')
        _updateExpression = 'SET ' + ', '.join(_updateExpressionParts)
        try:
            self.table.update_item(Key=_key,
                                   UpdateExpression=_updateExpression,
                                   ExpressionAttributeNames=_attributeNames,
                                   ExpressionAttributeValues=_attributeValues)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e) from e

    def create(self, *args, **kwargs):
        return self.update_or_create(*args, **kwargs)

    def latest(self, sequence_id: int) -> dict:
        """
        Retrieve the most recent event for a notification.

        Parameters
        ----------
        sequence_id : int

        Returns
        -------
        dict
        """
        response = self.table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('sequence').eq(sequence_id)
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
        app.config.setdefault('DYNAMODB_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', None)
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', None)
        app.config.setdefault('AWS_REGION', 'us-east-1')
        app.config.setdefault('VERSION', 'none')
        app.config.setdefault('DYNAMODB_VERIFY', 'true')
        app.config.setdefault('AWS_SESSION_TOKEN', None)

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            aws_session_token = self.app.config['AWS_SESSION_TOKEN']
            region_name = self.app.config['AWS_REGION']
            version = self.app.config['VERSION']
            verify = self.app.config['DYNAMODB_VERIFY'] == 'true'
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
            aws_session_token = os.environ.get('AWS_SESSION_TOKEN', None)
            region_name = os.environ.get('AWS_REGION', 'us-east-1')
            version = os.environ.get('VERSION', 'none')
            verify = os.environ.get('DYNAMODB_VERIFY', 'true') == 'true'
        return ExtractionEventSession(endpoint_url, aws_access_key,
                                      aws_secret_key, aws_session_token,
                                      region_name, version, verify=verify)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'extraction_events'):
                ctx.extraction_events = self.get_session()
                try:
                    ctx.extraction_events.create_table()
                except ClientError as e:
                    logger.info('Table already exists for extraction_events')
                    pass
            return ctx.extraction_events
        return self.get_session()     # No application context.


extractionEvents = ExtractionEvents()
