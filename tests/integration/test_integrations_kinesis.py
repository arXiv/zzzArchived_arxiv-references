import unittest
import shlex
import subprocess
import boto3
import requests
import os
import time
import json
from datetime import datetime, timedelta
import decimal

from http.server import BaseHTTPRequestHandler, HTTPServer
import socket
from threading import Thread
from botocore.exceptions import ClientError

import logging
logging.getLogger('boto').setLevel(logging.ERROR)
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)


class MockServerRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(requests.codes.ok)
        self.send_header('Content-type', 'application/pdf')
        self.end_headers()
        with open('tests/data/1702.07336.pdf', 'rb') as f:
            self.wfile.write(f.read())
        self.server.call_count_get += 1


class MockHTTPServer(HTTPServer):
    def __init__(self, *args, **kwargs):
        super(MockHTTPServer, self).__init__(*args, **kwargs)
        self.call_count_get = 0
        self.call_count_post = 0


def get_free_port():
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    address, port = s.getsockname()
    s.close()
    return port


class TestIntegrationWithKinesis(unittest.TestCase):
    """References agent subscribes to PDFIsAvailable stream."""

    @classmethod
    def setUpClass(cls):
        """Start localstack, mock proxy, create stream, and start agent."""

        cls.mock_server_port = get_free_port()
        cls.mock_server = MockHTTPServer(('localhost', cls.mock_server_port),
                                         MockServerRequestHandler)
        cls.mock_server_thread = Thread(target=cls.mock_server.serve_forever)
        cls.mock_server_thread.setDaemon(True)
        cls.mock_server_thread.start()

        pull = """docker pull atlassianlabs/localstack"""
        pull = shlex.split(pull)
        start = """docker run -it -p 4567-4578:4567-4578 -p 8080:8080 \
                    -e "USE_SSL=true" atlassianlabs/localstack"""
        start = shlex.split(start)
        subprocess.run(pull, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.Popen(start, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        cls.kinesis = boto3.client('kinesis', verify=False,
                                   endpoint_url='https://localhost:4568')
        try:
            cls.kinesis.create_stream(StreamName='PDFIsAvailable',
                                      ShardCount=1)
        except ClientError:
            pass

        arxiv_home = "https://localhost:%i" % cls.mock_server_port
        start_agent = """docker run -it --network host -e "MODE=test" \
                    -e "AWS_ACCESS_KEY_ID=asdf12345" \
                    -e "AWS_SECRET_ACCESS_KEY=qwertyuiop" \
                    -e "JAVA_FLAGS=-Dcom.amazonaws.sdk.disableCertChecking" \
                    -e "AWS_CBOR_DISABLE=1" \
                    -e "ARXIV_HOME={arxiv}" \
                    references-agent:test""".format(arxiv=arxiv_home)
        start_agent = shlex.split(start_agent)
        subprocess.Popen(start_agent)#, stdout=subprocess.PIPE,
                         #stderr=subprocess.PIPE)
        time.sleep(10)

    def test_agent_receives_notification(self):
        """A PDFIsAvailable notification is generated."""
        self.kinesis.put_record(
            StreamName='PDFIsAvailable',
            Data=bytes(json.dumps({
                "document_id": "1602.00123v3"
            }), encoding='utf-8'),
            PartitionKey='1'
        )
        time.sleep(2)
        self.assertEqual(self.mock_server.call_count_get, 1)

    @classmethod
    def tearDownClass(cls):
        """Spin down ES."""
        ps = "docker ps | grep references-agent | awk '{print $1;}'"
        container = subprocess.check_output(["/bin/sh", "-c", ps]) \
            .strip().decode('ascii')
        stop = """docker stop %s""" % container
        stop = shlex.split(stop)
        subprocess.run(stop, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
