"""Unit tests for the :mod:`references.agent.consumer` module."""
import unittest
from unittest import mock
import json
from amazon_kclpy import kcl

from references.agent import consumer


class TestRecordProcessor(unittest.TestCase):
    """Test the :meth:`.consumer.RecordProcessor.process_records` method."""

    def test_bad_data_are_ignored(self):
        """Malformed notification data are ignored."""
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

    def test_checkpoint_handles_ShutdownException(self):
        """
        Test the case that a ShutdownException is raised during processing.

        When a CheckpointError (ShutdownException) is raised, should not
        attempt to retry checkpointing.
        """
        checkpointer = mock.MagicMock()

        def _side_effect(seq, subseq):
            raise kcl.CheckpointError('ShutdownException')
        checkpointer.checkpoint = mock.MagicMock(side_effect=_side_effect)

        processor = consumer.RecordProcessor()
        processor.initialize(None)
        processor.checkpoint(checkpointer, 1, 2)

        self.assertEqual(checkpointer.checkpoint.call_count, 1)

    def test_checkpoint_handles_InvalidStateException(self):
        """
        Test the case that an InvalidStateException is raised.

        When a CheckpointError (InvalidStateException) is raised, should retry
        several times.
        """
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

    def test_checkpoint_handles_ThrottlingException(self):
        """
        Test the case that a ThrottlingException is raised.

        When a CheckpointError (ThrottlingException) is raised, should retry
        several times.
        """
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

    def test_true_if_largest_unset(self):
        """Return true if largest seq is not set."""
        processor = consumer.RecordProcessor()
        self.assertTrue(processor.should_update_sequence(1234, 5678))

    def test_true_if_seq_is_larger_than_previously_seen(self):
        """Return true if the current seq is larger than previously seen."""
        processor = consumer.RecordProcessor()
        processor._largest_seq = (1, 1)
        self.assertTrue(processor.should_update_sequence(2, 1))
        self.assertTrue(processor.should_update_sequence(1, 2))
        self.assertFalse(processor.should_update_sequence(1, 1))


class TestRecordProcessorShutdown(unittest.TestCase):
    """Test the handling of Shutdown signals."""

    def test_shutdown_terminate(self):
        """
        Test the case that a TERMINATE signal is passed.

        When :meth:`consumer.RecordProcessor` receives a TERMINATE shutdown
        signal, it should attempt to checkpoint.
        """
        checkpointer = mock.MagicMock()
        checkpointer.checkpoint = mock.MagicMock(return_value=None)
        shutdown = mock.MagicMock()
        shutdown.reason = 'TERMINATE'
        shutdown.checkpointer = checkpointer

        processor = consumer.RecordProcessor()
        processor.shutdown(shutdown)

        self.assertEqual(checkpointer.checkpoint.call_count, 1)

    def test_shutdown_zombie(self):
        """
        Test the case that a ZOMBIE signal is passed.

        When :meth:`consumer.RecordProcessor` receives a ZOMBIE shutdown
        signal, it should not attempt to checkpoint.
        """
        checkpointer = mock.MagicMock()
        checkpointer.checkpoint = mock.MagicMock(return_value=None)
        shutdown = mock.MagicMock()
        shutdown.reason = 'ZOMBIE'
        shutdown.checkpointer = checkpointer

        processor = consumer.RecordProcessor()
        processor.shutdown(shutdown)

        self.assertEqual(checkpointer.checkpoint.call_count, 0)


if __name__ == '__main__':
    unittest.main()
