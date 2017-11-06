import unittest
from unittest import mock
from moto import mock_dynamodb2
import shlex
import subprocess
from references.services import data_store
import os
import time

valid_data = [
    {
        "raw": "Peirson et al 2015 blah blah",
        "reftype": "citation"
    },
    {
        "raw": "Jones 2012",
        "reftype": "citation"
    },
    {
        "raw": "Majumdar 1968 etc",
        "reftype": "citation"
    },
    {
        "raw": "The brown fox, 1921",
        "reftype": "citation"
    }
]


class TestDataStoreIntegrationWithDynamoDB(unittest.TestCase):
    """Data store service integrates with DynamoDB."""

    @classmethod
    def setUpClass(cls):
        """Start localstack, initialize DynamoDB session."""
        pull = """docker pull atlassianlabs/localstack"""
        pull = shlex.split(pull)
        start = """docker run -it -p 4567-4578:4567-4578 -p 8080:8080 \
                    -e "USE_SSL=true" atlassianlabs/localstack"""
        start = shlex.split(start)
        subprocess.run(pull, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.Popen(start, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(10)    # Wait for localstack to be available.

        app = mock.MagicMock()
        app.config = {
            'DYNAMODB_ENDPOINT': 'https://localhost:4569',
            'DYNAMODB_VERIFY': 'false',
            'AWS_ACCESS_KEY_ID': 'foo',
            'AWS_SECRET_ACCESS_KEY': 'bar'
        }

    @mock.patch('references.services.data_store.current_app')
    def test_raw_extractions_integration(self, mock_app):
        document_id = '123.4566v8'
        extractor = 'baz_extractor'
        mock_app.config = {
            'DYNAMODB_ENDPOINT': 'https://localhost:4569',
            'DYNAMODB_VERIFY': 'false'
        }
        mock_app._get_current_object = mock.MagicMock(return_value=mock_app)
        data_store.init_app(mock_app)
        data_store.init_db()
        data_store.store_raw_extraction(document_id, extractor, valid_data)
        data = data_store.get_raw_extraction(document_id, extractor)
        self.assertEqual(data['document'], document_id)
        self.assertEqual(data['extractor'], extractor)
        self.assertListEqual(data['references'], valid_data)

    @classmethod
    def tearDownClass(cls):
        """Spin down ES."""
        ps = "docker ps | grep references-agent | awk '{print $1;}'"
        container = subprocess.check_output(["/bin/sh", "-c", ps]) \
            .strip().decode('ascii')
        stop = """docker stop %s""" % container
        stop = shlex.split(stop)
        subprocess.run(stop, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
