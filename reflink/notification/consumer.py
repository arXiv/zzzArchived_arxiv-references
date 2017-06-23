"""
This script is called by the KCL MultiLangDaemon to process Kinesis streams.

http://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-implementation-app-py.html
https://github.com/awslabs/amazon-kinesis-client-python/blob/master/samples/sample_kclpy_app.py
"""

import sys
import time
import logging
import os
import json
from reflink.tasks import orchestrate

# TODO: make this configurable.
logging.basicConfig(filename=__name__,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    level=logging.DEBUG)

import amazon_kclpy
from amazon_kclpy import kcl
from amazon_kclpy.v2 import processor
from amazon_kclpy.messages import ProcessRecordsInput, ShutdownInput


class RecordProcessor(processor.RecordProcessorBase):
    """
    Processes records received by the Kinesis consumer.

    This class is instantiated when the containing script is run by the
    MultiLangDaemon. The underlying KCL process handles threading, offset
    tracking, etc. The KCL guarantees that our :class:`.RecordProcessor` will
    see each record *at least* once; the checkpoint mechanism gives us a way to
    further guarantee that we only process each record a single time.

    See the ``consumer.properties`` file for configuration details, including
    streams.
    """
    def __init__(self):
        self._SLEEP_SECONDS = 5
        self._CHECKPOINT_RETRIES = 5
        self._CHECKPOINT_FREQ = 60
        self._largest_seq = (None, None)
        self._largest_sub_seq = None
        self._last_checkpoint_time = None

    def initialize(self, initialize_input):
        """
        Called once by a KCLProcess before any calls to process_records.
        """
        self._largest_seq = (None, None)
        self._last_checkpoint_time = time.time()

    def checkpoint(self, checkpointer: amazon_kclpy.kcl.Checkpointer,
                   sequence_number: bytes = None,
                   sub_sequence_number: int = None) -> None:
        """
        Checkpoints with retries on retryable exceptions.
        """
        for n in range(0, self._CHECKPOINT_RETRIES):
            try:
                checkpointer.checkpoint(sequence_number, sub_sequence_number)
                return
            except kcl.CheckpointError as e:
                if 'ShutdownException' == e.value:
                    # A ShutdownException indicates that this record processor
                    #  should be shutdown. This is due to some failover event,
                    #  e.g. another MultiLangDaemon has taken the lease for
                    #  this shard.
                    logging.info("Encountered shutdown exception, skipping"
                                 " checkpoint")
                    return
                elif 'ThrottlingException' == e.value:
                    # A ThrottlingException indicates that one of our
                    #  dependencies is is over burdened, e.g. too many dynamo
                    #  writes. We will sleep temporarily to let it recover.
                    if self._CHECKPOINT_RETRIES - 1 == n:
                        logging.error("Failed to checkpoint after %i attempts,"
                                      " giving up." % n)
                        return
                    else:
                        logging.info("Was throttled while checkpointing, will"
                                     " attempt again in %i seconds"
                                     % self._SLEEP_SECONDS)
                elif 'InvalidStateException' == e.value:
                    logging.error("MultiLangDaemon reported an invalid state"
                                  " while checkpointing.")
                else:  # Some other error
                    logging.error("Encountered an error while checkpointing,"
                                  " error was %s" % e)
            time.sleep(self._SLEEP_SECONDS)

    def process_record(self, data: bytes, partition_key: bytes,
                       sequence_number: int, sub_sequence_number: int) -> None:
        """
        Called for each record that is passed to process_records.

        Parameters
        ----------
        data : bytes
        partition_key : bytes
        sequence_number : int
        sub_sequence_number : int
        """

        try:
            deserialized = json.loads(data.decode('utf-8'))
        except Exception as e:
            logging.error("Error while deserializing data: %s" % e)
            logging.error("Data payload: %s" % data)

        try:
            orchestrate.process_document(deserialized.get('document_id'))
        except Exception as e:
            logging.error("Error while processing document: %s" % e)
            logging.error("Data payload: %s" % data)

    def should_update_sequence(self, sequence_number: int,
                               sub_sequence_number: int) -> bool:
        """
        Determines whether a new larger sequence number is available

        Parameters
        ----------
        sequence_number : int
        sub_sequence_number : int

        Returns
        -------
        bool
        """
        return self._largest_seq == (None, None) or \
               sequence_number > self._largest_seq[0] or \
               (sequence_number == self._largest_seq[0] and \
                sub_sequence_number > self._largest_seq[1])

    def process_records(self, records: ProcessRecordsInput) -> None:
        """
        Called by a KCLProcess with a list of records to be processed and a
        checkpointer which accepts sequence numbers from the records to
        indicate where in the byteseam to checkpoint.

        Parameters
        ----------
        records : :class:`amazon_kclpy.messages.ProcessRecordsInput`
        """
        try:
            for record in records.records:
                data = record.binary_data
                seq = int(record.sequence_number)
                sub_seq = record.sub_sequence_number
                key = record.partition_key
                self.process_record(data, key, seq, sub_seq)
                if self.should_update_sequence(seq, sub_seq):
                    self._largest_seq = (seq, sub_seq)

            # Checkpoints every self._CHECKPOINT_FREQ seconds
            if time.time() - self._last_checkpoint_time > self._CHECKPOINT_FREQ:
                self.checkpoint(records.checkpointer,
                                bytes(self._largest_seq[0]),
                                self._largest_seq[1])
                self._last_checkpoint_time = time.time()

        except Exception as e:
            logging.error("Encountered an exception while processing records."
                          " Exception was %s" % e)

    def shutdown(self, shutdown: ShutdownInput) -> None:
        """
        Called by a KCLProcess instance to indicate that this record processor
        should shutdown. After this is called, there will be no more calls to
        any other methods of this record processor.

        Parameters
        ----------
        shutdown : :class:`amazon_kclpy.messages.ShutdownInput`
        """
        try:
            if shutdown.reason == 'TERMINATE':
                # **THE RECORD PROCESSOR MUST CHECKPOINT OR THE KCL WILL BE
                #   UNABLE TO PROGRESS**
                # Checkpointing with no parameter will checkpoint at the
                # largest sequence number reached by this processor on this
                # shard id.
                logging.info("Was told to terminate, attempting to checkpoint.")
                self.checkpoint(shutdown.checkpointer, None)
            else: # reason == 'ZOMBIE'
                # **ATTEMPTING TO CHECKPOINT ONCE A LEASE IS LOST WILL FAIL**
                logging.info("Shutting down due to failover. Won't checkpoint.")
        except Exception as e:
            logging.error("Encountered exception while shutting down: %s" % e)


if __name__ == "__main__":
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()
