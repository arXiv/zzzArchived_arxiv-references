"""Creates the PDFIsAvailable stream if it does not already exist."""
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
import os
from references.services import credentials
from references.factory import create_web_app
from references.context import get_application_config
import sys


if __name__ == '__main__':
    app = create_web_app()
    app.app_context().push()
    config = get_application_config()

    access_key, secret, token = credentials.get_credentials()
    endpoint = config.get('KINESIS_ENDPOINT')
    region = config.get('AWS_REGION', 'us-east-1')
    verify = config.get('KINESIS_VERIFY') == 'true'
    stream_name = config.get('KINESIS_STREAM', 'PDFIsAvailable')

    config = Config(read_timeout=5, connect_timeout=5)
    client = boto3.client('kinesis', region_name=region, endpoint_url=endpoint,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret,
                          aws_session_token=token,
                          verify=verify, config=config)
    try:
        client.describe_stream(StreamName='PDFIsAvailable')
    except ClientError:
        client.create_stream(StreamName='PDFIsAvailable', ShardCount=1)
