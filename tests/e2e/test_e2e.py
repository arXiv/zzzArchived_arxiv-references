"""End-to-end tests."""

import unittest
from unittest import mock
import os
import time
import boto3
from botocore.exceptions import ClientError
from urllib3 import Retry
import requests
from urllib.parse import urljoin
import logging
import json
import jsonschema
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logging.getLogger('boto').setLevel(logging.ERROR)
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "foo")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "bar")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", None)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
CLOUDWATCH_ENDPOINT = os.environ.get("CLOUDWATCH_ENDPOINT",
                                     "https://references-test-localstack:4582")
DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT",
                                   "https://references-test-localstack:4569")
KINESIS_ENDPOINT = os.environ.get("KINESIS_ENDPOINT",
                                  "https://references-test-localstack:4568")
KINESIS_VERIFY = os.environ.get("KINESIS_VERIFY", "false") == "true"
KINESIS_STREAM = os.environ.get('KINESIS_STREAM', 'PDFIsAvailable')
DYNAMODB_VERIFY = os.environ.get("DYNAMODB_VERIFY", "false") == "true"
CLOUDWATCH_VERIFY = os.environ.get("CLOUDWATCH_VERIFY", "false") == "true"
EXTRACTION_ENDPOINT = os.environ.get("EXTRACTION_ENDPOINT",
                                     "http://references-api:8000")
LOGLEVEL = os.environ.get("LOGLEVEL", 10)
LOGFILE = os.environ.get("LOGFILE", "/var/log/references-agent-processor.log")
MODE = os.environ.get("MODE", "test")
JAVA_FLAGS = os.environ.get("JAVA_FLAGS",
                            "-Dcom.amazonaws.sdk.disableCertChecking")
AWS_CBOR_DISABLE = os.environ.get("AWS_CBOR_DISABLE", "true")

RAW_TABLE_NAME = os.environ.get("RAW_TABLE_NAME", 'RawExtractions')
EXTRACTIONS_TABLE_NAME = os.environ.get("EXTRACTIONS_TABLE_NAME",
                                        'Extractions')
REFERENCES_TABLE_NAME = os.environ.get("REFERENCES_TABLE_NAME",
                                       'StoredReference')


with open('schema/StoredReference.json') as f:
    stored_references_schema = json.load(f)


class TestReferenceConsumer(unittest.TestCase):
    """Exercise the reference extraction agent (Kinesis consumer)."""

    @classmethod
    def setUpClass(cls):
        """Initialize the kinesis stream."""
        cls.client = boto3.client('kinesis', verify=KINESIS_VERIFY,
                                  region_name=AWS_REGION,
                                  endpoint_url=KINESIS_ENDPOINT,
                                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                  aws_session_token=AWS_SESSION_TOKEN)
        try:
            cls.client.create_stream(StreamName=KINESIS_STREAM, ShardCount=1)
        except ClientError as e:
            pass

        cls._session = requests.Session()
        cls._adapter = requests.adapters.HTTPAdapter(
            max_retries=Retry(connect=30, read=10, backoff_factor=5))
        cls._session.mount('http://', cls._adapter)

    @mock.patch('flask.current_app')
    def test_process_record(self, mock_app):
        """Initiate extraction via the agent."""
        mock_app.config = {
            'DYNAMODB_ENDPOINT': DYNAMODB_ENDPOINT,
            'DYNAMODB_VERIFY': DYNAMODB_VERIFY,
            'CLOUDWATCH_ENDPOINT': CLOUDWATCH_ENDPOINT,
            'CLOUDWATCH_VERIFY': CLOUDWATCH_VERIFY,
            'AWS_REGION': AWS_REGION,
            'RAW_TABLE_NAME': RAW_TABLE_NAME,
            'EXTRACTIONS_TABLE_NAME': EXTRACTIONS_TABLE_NAME,
            'REFERENCES_TABLE_NAME': REFERENCES_TABLE_NAME,
            'INSTANCE_CREDENTIALS': 'nope',
            'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        }
        mock_app._get_current_object = mock.MagicMock(return_value=mock_app)

        from references.services import data_store
        data_store.init_app(mock_app)
        data_store.init_db()

        document_id = '1606.00123'
        payload = json.dumps({
            "document_id": document_id,
            "url": "https://arxiv.org/pdf/%s" % document_id
        }).encode('utf-8')
        self.client.put_record(StreamName='PDFIsAvailable', Data=payload,
                               PartitionKey='0')

        time.sleep(30)
        target = urljoin(EXTRACTION_ENDPOINT, '/references/%s' % document_id)
        response = self._session.get(target)
        retries = 0
        while response.status_code != 200:
            if retries > 5:
                self.fail('Record not processed')
            time.sleep(10)
            response = self._session.get(target)
            retries += 1


class TestReferenceExtractionViaAPI(unittest.TestCase):
    @classmethod
    @mock.patch('flask.current_app')
    def setUpClass(cls, mock_app):
        status_endpoint = urljoin(EXTRACTION_ENDPOINT, "/status")
        logger.debug('Check status at %s' % status_endpoint)

        cls._session = requests.Session()
        cls._adapter = requests.adapters.HTTPAdapter(
            max_retries=Retry(connect=30, read=10, backoff_factor=5))
        cls._session.mount('http://', cls._adapter)

        response = cls._session.get(status_endpoint, timeout=1)
        if response.status_code != 200:
            raise IOError('ack!')

        mock_app.config = {
            'DYNAMODB_ENDPOINT': DYNAMODB_ENDPOINT,
            'DYNAMODB_VERIFY': DYNAMODB_VERIFY,
            'CLOUDWATCH_ENDPOINT': CLOUDWATCH_ENDPOINT,
            'CLOUDWATCH_VERIFY': CLOUDWATCH_VERIFY,
            'AWS_REGION': AWS_REGION,
            'RAW_TABLE_NAME': RAW_TABLE_NAME,
            'EXTRACTIONS_TABLE_NAME': EXTRACTIONS_TABLE_NAME,
            'REFERENCES_TABLE_NAME': REFERENCES_TABLE_NAME,
            'INSTANCE_CREDENTIALS': 'nope',
            'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        }
        mock_app._get_current_object = mock.MagicMock(return_value=mock_app)

        from references.services import data_store
        data_store.init_app(mock_app)
        data_store.init_db()
        cls.dyn = boto3.client('dynamodb', verify=DYNAMODB_VERIFY,
                               region_name=AWS_REGION,
                               endpoint_url=DYNAMODB_ENDPOINT,
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               aws_session_token=AWS_SESSION_TOKEN)

    def test_extraction_via_api(self):
        """Exercise full extraction lifecycle via API."""
        document_id = "1606.00125"
        payload = {
            "document_id": document_id,
            "url": "https://arxiv.org/pdf/1606.00125"
        }
        start_time = datetime.now()
        _target = urljoin(EXTRACTION_ENDPOINT, "/references")
        response = self._session.post(_target, json=payload)
        self.assertEqual(response.status_code, 202,
                         "A valid POST request is accepted")
        redirect_url = response.headers.get('Location', None)
        if redirect_url is None:
            self.fail("Accepted response should include status URL")

        target_url = urljoin(EXTRACTION_ENDPOINT,
                             '/references/%s' % document_id)
        response = self._session.get(redirect_url)
        self.assertTrue(response.status_code in [200, 303],
                        "Status endpoint should be available")
        while not response.url.startswith(target_url):
            response = self._session.get(response.url)
            if response.status_code not in [200, 303]:
                self.fail("Status check should return OK or redirect.")

            logger.debug('Response: %i, %s' %
                         (response.status_code, response.content))
            if datetime.now() - start_time > timedelta(minutes=3):
                self.fail('Extraction should complete in reasonable time')

        # POST again with the same request.
        try:
            _target = urljoin(EXTRACTION_ENDPOINT, "/references")
            response = self._session.post(_target, json=payload)
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            _target = urljoin(EXTRACTION_ENDPOINT, "/references")
            response = self._session.post(_target, json=payload)
        self.assertEqual(response.status_code, 200,
                         "Response status for duplicate request should be OK")
        target_url = urljoin(EXTRACTION_ENDPOINT,
                             '/references/%s' % document_id)
        self.assertEqual(response.url, target_url,
                         "Response for duplicate request should redirect to"
                         " extraction URL")

        # Now get generated resources.
        try:
            response = self._session.get(urljoin(EXTRACTION_ENDPOINT,
                                         '/references/%s' % document_id))
        except Exception as e:
            logger.error(e)
            self.fail('Document references endpoint should be available')
        self.assertEqual(response.status_code, 200,
                         "Document references endpoint should respond with OK")

        content = response.json()
        for ref in content.get('references'):
            jsonschema.validate(content, ref)

        extractors = content.get('extractors')
        if extractors is None:
            self.fail('At least one extractor should be included')
        for extractor, path in extractors.items():
            response = self._session.get(urljoin(EXTRACTION_ENDPOINT, path))
            self.assertEqual(response.status_code, 200,
                             "Raw extraction metadata should be available.")

        ref = content.get('references')[0]
        path = '/references/%s/ref/%s' % (document_id, ref.get('identifier'))
        try:
            response = self._session.get(urljoin(EXTRACTION_ENDPOINT, path))
        except Exception as e:
            logger.error(e)
            self.fail('Individual reference endpoint should be available')
        self.assertEqual(response.status_code, 200,
                         "Individual reference endpoint should respond OK")

    def test_incomplete_requests_are_rejected(self):
        """Extraction is requested without a document id."""
        payload = {
            "url": "https://arxiv.org/pdf/1606.00125"
        }
        _target = urljoin(EXTRACTION_ENDPOINT, "/references")
        response = self._session.post(_target, json=payload)
        self.assertEqual(response.status_code, 400,
                         "Incomplete requests should not be accepted")

    def test_invalid_urls_are_rejected(self):
        """Extraction is requested for a URL in an untrusted domain."""
        document_id = "1606.00125"
        payload = {
            "document_id": document_id,
            "url": "https://asdf.org/pdf/1606.00125"
        }
        _target = urljoin(EXTRACTION_ENDPOINT, "/references")
        response = self._session.post(_target, json=payload)
        self.assertEqual(response.status_code, 400,
                         "Requests with untrusted URLs should be rejected")

    def test_nonexistant_document(self):
        """Reference data are requested for an unknown document."""
        response = self._session.get(urljoin(EXTRACTION_ENDPOINT,
                                             '/references/1234.5678'))
        self.assertEqual(response.status_code, 404,
                         "Response should have status 404")

    @classmethod
    def tearDownClass(cls):
        """Remove DynamoDB tables."""
        cls.dyn.delete_table(TableName=RAW_TABLE_NAME)
        cls.dyn.delete_table(TableName=EXTRACTIONS_TABLE_NAME)
        cls.dyn.delete_table(TableName=REFERENCES_TABLE_NAME)
