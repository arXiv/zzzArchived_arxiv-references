"""End-to-end tests."""

import unittest
from unittest import mock
import os
import time
import boto3
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


class TestReferenceExtractionViaAPI(unittest.TestCase):
    @classmethod
    @mock.patch('references.services.data_store.current_app')
    def setUpClass(cls, mock_app):
        status_endpoint = urljoin(EXTRACTION_ENDPOINT, "/status")
        logger.debug('Check status at %s' % status_endpoint)
        response = requests.get(status_endpoint, timeout=1)
        if not response.status_code == 200:
            logger.error('Could not connect at %s' % status_endpoint)

        mock_app.config = {
            'DYNAMODB_ENDPOINT': DYNAMODB_ENDPOINT,
            'DYNAMODB_VERIFY': DYNAMODB_VERIFY,
            'CLOUDWATCH_ENDPOINT': CLOUDWATCH_ENDPOINT,
            'CLOUDWATCH_VERIFY': CLOUDWATCH_VERIFY,
            'AWS_REGION': AWS_REGION
        }
        mock_app._get_current_object = mock.MagicMock(return_value=mock_app)

        from references.services import data_store
        data_store.referencesStore.init_app(mock_app)
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
        response = requests.post(urljoin(EXTRACTION_ENDPOINT, "/references"),
                                 json=payload)
        self.assertEqual(response.status_code, 202,
                         "A valid POST request is accepted")
        redirect_url = response.headers.get('Location', None)
        if redirect_url is None:
            self.fail("Accepted response should include status URL")

        target_url = urljoin(EXTRACTION_ENDPOINT,
                             '/references/%s' % document_id)
        response = requests.get(redirect_url)
        self.assertTrue(response.status_code in [200, 303],
                        "Status endpoint should be available")
        while not response.url.startswith(target_url):
            response = requests.get(response.url)
            if response.status_code not in [200, 303]:
                self.fail("Status check should return OK or redirect.")

            logger.debug('Response: %i, %s' %
                         (response.status_code, response.content))
            time.sleep(4)
            if datetime.now() - start_time > timedelta(minutes=3):
                self.fail('Extraction should complete in reasonable time')

        # POST again with the same request.
        response = requests.post(urljoin(EXTRACTION_ENDPOINT, "/references"),
                                 json=payload)
        self.assertEqual(response.status_code, 200,
                         "Response status for duplicate request should be OK")
        target_url = urljoin(EXTRACTION_ENDPOINT,
                             '/references/%s' % document_id)
        self.assertEqual(response.url, target_url,
                         "Response for duplicate request should redirect to"
                         " extraction URL")

        # Now get generated resources.
        try:
            response = requests.get(urljoin(EXTRACTION_ENDPOINT,
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
            response = requests.get(urljoin(EXTRACTION_ENDPOINT, path))
            self.assertEqual(response.status_code, 200,
                             "Raw extraction metadata should be available.")

        ref = content.get('references')[0]
        path = '/references/%s/ref/%s' % (document_id, ref.get('identifier'))
        try:
            response = requests.get(urljoin(EXTRACTION_ENDPOINT, path))
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
        response = requests.post(urljoin(EXTRACTION_ENDPOINT, "/references"),
                                 json=payload)
        self.assertEqual(response.status_code, 400,
                         "Incomplete requests should not be accepted")

    def test_invalid_urls_are_rejected(self):
        """Extraction is requested for a URL in an untrusted domain."""
        document_id = "1606.00125"
        payload = {
            "document_id": document_id,
            "url": "https://asdf.org/pdf/1606.00125"
        }
        response = requests.post(urljoin(EXTRACTION_ENDPOINT, "/references"),
                                 json=payload)
        self.assertEqual(response.status_code, 400,
                         "Requests with untrusted URLs should be rejected")

    def test_nonexistant_document(self):
        """Reference data are requested for an unknown document."""
        response = requests.get(urljoin(EXTRACTION_ENDPOINT,
                                        '/references/1234.5678'))
        self.assertEqual(response.status_code, 404,
                         "Response should have status 404")

    @classmethod
    def tearDownClass(cls):
        """Remove DynamoDB tables."""
        cls.dyn.delete_table(TableName=RAW_TABLE_NAME)
        cls.dyn.delete_table(TableName=EXTRACTIONS_TABLE_NAME)
        cls.dyn.delete_table(TableName=REFERENCES_TABLE_NAME)
