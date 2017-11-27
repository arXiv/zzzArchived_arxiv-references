"""Unit tests for the :mod:`references.agent.consumer` module."""

import unittest
from unittest import mock
import os
import json
from amazon_kclpy import kcl

from references.agent import consumer


def mock_extractor_session(mock_obj):
    """Set up mock responses for GET and POST requests to extraction API."""
    mock_get_response = mock.MagicMock(status_code=200, ok=True)
    mock_get = mock.MagicMock(return_value=mock_get_response)
    mock_session_instance = mock.MagicMock()
    type(mock_session_instance).get = mock_get

    raw = json.dumps({'foo': 'bar'})
    mock_response = mock.MagicMock(content=raw, status_code=200)
    mock_post = mock.MagicMock(return_value=mock_response)
    type(mock_session_instance).post = mock_post
    mock_obj.return_value = mock_session_instance


class TestRecordProcessor(unittest.TestCase):
    """Test the :meth:`.consumer.RecordProcessor.process_records` method."""

    @classmethod
    def setUpClass(cls):
        os.environ['INSTANCE_CREDENTIALS'] = ''
        os.environ['EXTRACTION_ENDPOINT'] = 'http://foo.bar'

    @mock.patch('references.services.extractor.requests.Session')
    def test_bad_data_are_ignored(self, mock_session):
        """Malformed notification data are ignored."""
        mock_extractor_session(mock_session)

        processor = consumer.RecordProcessor()
        processor.initialize(None)
        records = mock.MagicMock()
        records.checkpointer = mock.MagicMock()
        record_mock = mock.MagicMock()
        record_mock.binary_data = bytes(str({'foo': 'bar', 'baz': 'bat'}),
                                        encoding='utf-8')
        records.records = [record_mock]
        try:
            processor.process_records(records)
        except Exception as e:
            self.fail('raised %s' % e)


class TestRecordProcessorCheckpoint(unittest.TestCase):
    """Test the functionality of the checkpoint mechanism."""

    @classmethod
    def setUpClass(cls):
        os.environ['INSTANCE_CREDENTIALS'] = ''
        os.environ['EXTRACTION_ENDPOINT'] = 'http://foo.bar'

    @mock.patch('references.services.extractor.requests.Session')
    def test_checkpoint_on_time(self, mock_session):
        """Test the case that time >_CHECKPOINT_FREQ has passed."""
        mock_extractor_session(mock_session)

        processor = consumer.RecordProcessor()
        processor.initialize(None)
        retries = 5
        processor._SLEEP_SECONDS = 0.1    # So that we don't wait all day.
        processor._CHECKPOINT_RETRIES = retries
        processor._CHECKPOINT_FREQ = 0
        processor._largest_seq = (1, 1)
        processor._largest_sub_seq = 1

        records = mock.MagicMock()
        records.checkpointer = mock.MagicMock()
        bdata = bytes(str({'foo': 'bar', 'baz': 'bat'}), encoding='utf-8')
        records.records = [
            mock.MagicMock(sequence_number=12345, sub_sequence_number=0,
                           binary_data=bdata),
            mock.MagicMock(sequence_number=12346, sub_sequence_number=0,
                           binary_data=bdata),
            mock.MagicMock(sequence_number=12347, sub_sequence_number=0,
                           binary_data=bdata),
        ]
        processor.process_records(records)
        self.assertEqual(records.checkpointer.checkpoint.call_count, 1,
                         "Checkpoint method on checkpointer is called")

    @mock.patch('references.services.extractor.requests.Session')
    def test_checkpoint_handles_ShutdownException(self, mock_session):
        """
        Test the case that a ShutdownException is raised during processing.

        When a CheckpointError (ShutdownException) is raised, should not
        attempt to retry checkpointing.
        """
        mock_extractor_session(mock_session)
        checkpointer = mock.MagicMock()

        def _side_effect(seq, subseq):
            raise kcl.CheckpointError('ShutdownException')
        checkpointer.checkpoint = mock.MagicMock(side_effect=_side_effect)

        processor = consumer.RecordProcessor()
        processor.initialize(None)
        processor.checkpoint(checkpointer, 1, 2)

        self.assertEqual(checkpointer.checkpoint.call_count, 1)

    @mock.patch('references.services.extractor.requests.Session')
    def test_checkpoint_handles_InvalidStateException(self, mock_session):
        """
        Test the case that an InvalidStateException is raised.

        When a CheckpointError (InvalidStateException) is raised, should retry
        several times.
        """
        mock_extractor_session(mock_session)
        checkpointer = mock.MagicMock()

        def _side_effect(seq, subseq):
            raise kcl.CheckpointError('InvalidStateException')
        checkpointer.checkpoint = mock.MagicMock(side_effect=_side_effect)

        processor = consumer.RecordProcessor()
        processor.initialize(None)
        retries = 5
        processor._SLEEP_SECONDS = 0.1    # So that we don't wait all day.
        processor._CHECKPOINT_RETRIES = retries
        processor.checkpoint(checkpointer, 1, 2)

        self.assertEqual(checkpointer.checkpoint.call_count, retries)

    @mock.patch('references.services.extractor.requests.Session')
    def test_checkpoint_handles_ThrottlingException(self, mock_session):
        """
        Test the case that a ThrottlingException is raised.

        When a CheckpointError (ThrottlingException) is raised, should retry
        several times.
        """
        mock_extractor_session(mock_session)
        checkpointer = mock.MagicMock()

        def _side_effect(seq, subseq):
            raise kcl.CheckpointError('ThrottlingException')
        checkpointer.checkpoint = mock.MagicMock(side_effect=_side_effect)

        processor = consumer.RecordProcessor()
        processor.initialize(None)
        retries = 5
        processor._SLEEP_SECONDS = 0.1    # So that we don't wait all day.
        processor._CHECKPOINT_RETRIES = retries
        processor.checkpoint(checkpointer, 1, 2)

        self.assertEqual(checkpointer.checkpoint.call_count, retries)


class TestRecordProcessorShouldUpdateSequence(unittest.TestCase):
    """Tests for :meth:`consumer.RecordProcessor.should_update_sequence`."""

    @classmethod
    def setUpClass(cls):
        os.environ['INSTANCE_CREDENTIALS'] = ''
        os.environ['EXTRACTION_ENDPOINT'] = 'http://foo.bar'

    @mock.patch('references.services.extractor.requests.Session')
    def test_true_if_largest_unset(self, mock_session):
        """Return true if largest seq is not set."""
        mock_extractor_session(mock_session)
        processor = consumer.RecordProcessor()
        self.assertTrue(processor.should_update_sequence(1234, 5678))

    @mock.patch('references.services.extractor.requests.Session')
    def test_true_if_seq_is_larger_than_previously_seen(self, mock_session):
        """Return true if the current seq is larger than previously seen."""
        mock_extractor_session(mock_session)
        processor = consumer.RecordProcessor()
        processor._largest_seq = (1, 1)
        self.assertTrue(processor.should_update_sequence(2, 1))
        self.assertTrue(processor.should_update_sequence(1, 2))
        self.assertFalse(processor.should_update_sequence(1, 1))


class TestRecordProcessorShutdown(unittest.TestCase):
    """Test the handling of Shutdown signals."""

    @classmethod
    def setUpClass(cls):
        os.environ['INSTANCE_CREDENTIALS'] = ''
        os.environ['EXTRACTION_ENDPOINT'] = 'http://foo.bar'

    @mock.patch('references.services.extractor.requests.Session')
    def test_shutdown_terminate(self, mock_session):
        """
        Test the case that a TERMINATE signal is passed.

        When :meth:`consumer.RecordProcessor` receives a TERMINATE shutdown
        signal, it should attempt to checkpoint.
        """
        mock_extractor_session(mock_session)
        checkpointer = mock.MagicMock()
        checkpointer.checkpoint = mock.MagicMock(return_value=None)
        shutdown = mock.MagicMock()
        shutdown.reason = 'TERMINATE'
        shutdown.checkpointer = checkpointer

        processor = consumer.RecordProcessor()
        processor.shutdown(shutdown)

        self.assertEqual(checkpointer.checkpoint.call_count, 1)

    @mock.patch('references.services.extractor.requests.Session')
    def test_shutdown_zombie(self, mock_session):
        """
        Test the case that a ZOMBIE signal is passed.

        When :meth:`consumer.RecordProcessor` receives a ZOMBIE shutdown
        signal, it should not attempt to checkpoint.
        """
        mock_extractor_session(mock_session)
        checkpointer = mock.MagicMock()
        checkpointer.checkpoint = mock.MagicMock(return_value=None)
        shutdown = mock.MagicMock()
        shutdown.reason = 'ZOMBIE'
        shutdown.checkpointer = checkpointer

        processor = consumer.RecordProcessor()
        processor.shutdown(shutdown)

        self.assertEqual(checkpointer.checkpoint.call_count, 0)
