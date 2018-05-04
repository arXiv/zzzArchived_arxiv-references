"""Unit tests for :mod:`search.agent`."""

from unittest import TestCase, mock
import json
from references.agent import consumer
from arxiv.base.agent import process_stream


class TestExtractionAgent(TestCase):
    """Tests for :class:`.ExtractionAgent`."""

    def setUp(self):
        """Define a testing config."""
        self.config = {
            'KINESIS_STREAM': 'fooStream',
            'KINESIS_SHARD_ID': 'shard-0000000',
            'AWS_ACCESS_KEY_ID': 'ack',
            'AWS_SECRET_ACCESS_KEY': 'qwerty',
            'AWS_REGION': 'su-tsae-9'
        }
        self.checkpointer = mock.MagicMock()
        self.args = ('foo', '1', 'a1b2c3d4', 'qwertyuiop', 'us-east-1',
                     self.checkpointer)

    @mock.patch('boto3.client')
    @mock.patch('references.agent.consumer.tasks')
    def test_process_record_method(self, mock_tasks, mock_client_factory):
        """:meth:`.ExtractionAgent.process_record` accepts a Kinesis record."""
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client_factory.return_value = mock_client
        processor = consumer.ExtractionAgent(*self.args)

        raw = json.dumps({'document_id': '00123.45678v5'}).encode('utf-8')
        record = {'Data': raw, 'SequenceNumber': '1'}
        processor.process_record(record)

        self.assertEqual(mock_tasks.process_document.call_count, 1,
                         "Should call process_document")
        args, kwargs = mock_tasks.process_document.call_args
        self.assertEqual(args[0], '00123.45678v5',
                         "Should pass the document ID in the record")
        self.assertEqual(args[1], 'https://arxiv.org/pdf/00123.45678v5',
                         "Should pass a URL based on the document ID")

    @mock.patch('boto3.client')
    @mock.patch('references.agent.consumer.tasks')
    def test_process_stream(self, mock_tasks, mock_client_factory):
        """Run :func:`.process_stream` with a :class:`.ExtractionAgent`."""
        mock_client = mock.MagicMock()
        mock_client_factory.return_value = mock_client
        mock_client.get_shard_iterator.return_value = {'ShardIterator': '1'}

        class Stream(object):
            def __init__(self):
                self.max_records = 20
                self.yielded = 0

            def get_records(self, *args, **kwargs):
                to_yield = min(self.max_records, self.yielded + 10)
                records = {
                    "Records": [
                        {
                            'SequenceNumber': f'{i}',
                            'Data': json.dumps({
                                'document_id': f'{i}v5'
                            }).encode('utf-8')
                        } for i in range(self.yielded, to_yield)
                    ],
                    "NextShardIterator": f"{to_yield + 1}"
                }
                self.yielded = to_yield
                return records
        stream = Stream()
        mock_client.get_records.side_effect = stream.get_records

        class Checkpoint(object):
            def __init__(self):
                self.position = None

            def checkpoint(self, position):
                self.position = position

        process_stream(consumer.ExtractionAgent, self.config, Checkpoint(), 20)
        self.assertEqual(mock_tasks.process_document.call_count, 20,
                         "Should call process_document for each record")
        self.assertEqual(mock_tasks.process_document.call_count,
                         stream.yielded,
                         "Should call process_document for each record")
