"""

http://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-implementation-app-py.html
https://github.com/awslabs/amazon-kinesis-client-python/blob/master/samples/sample_kclpy_app.py
"""

import sys
import time
import logging
import os

# TODO: make this configurable.
logging.basicConfig(filename=__name__,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    level=logging.DEBUG)

from amazon_kclpy import kcl
from amazon_kclpy.v2 import processor
from amazon_kclpy.messages import ProcessRecordsInput


class RecordProcessor(processor.RecordProcessor):
    def __init__(self):
        self._SLEEP_SECONDS = 5
        self._CHECKPOINT_RETRIES = 5
        self._CHECKPOINT_FREQ_SECONDS = 60
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
                   sequence_number: str = None,
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
                        sys.stderr.write('Failed to checkpoint after {n} attempts, giving up.\n'.format(n=n))
                        return
                    else:
                        logging.info('Was throttled while checkpointing, will attempt again in {s} seconds'
                              .format(s=self._SLEEP_SECONDS))
                elif 'InvalidStateException' == e.value:
                    sys.stderr.write('MultiLangDaemon reported an invalid state while checkpointing.\n')
                else:  # Some other error
                    sys.stderr.write('Encountered an error while checkpointing, error was {e}.\n'.format(e=e))
            time.sleep(self._SLEEP_SECONDS)

    def process_record(self, data: str, partition_key: str,
                       sequence_number: int, sub_sequence_number: int) -> None:
        """
        Called for each record that is passed to process_records.
        """


        return

    def should_update_sequence(self, sequence_number: int,
                               sub_sequence_number: int) -> bool:
        """
        Determines whether a new larger sequence number is available
        """
        return any([
            self._largest_seq == (None, None),
            sequence_number > self._largest_seq[0],
            all([
                sequence_number == self._largest_seq[0],
                sub_sequence_number > self._largest_seq[1]
            ])
         ])

    def process_records(self, records: ProcessRecordsInput) -> None:
        """
        Called by a KCLProcess with a list of records to be processed and a
        checkpointer which accepts sequence numbers from the records to
        indicate where in the stream to checkpoint.
        :param  process_records_input: the records, and metadata about the
            records.
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

            # Checkpoints every self._CHECKPOINT_FREQ_SECONDS seconds
            if time.time() - self._last_checkpoint_time > self._CHECKPOINT_FREQ_SECONDS:
                self.checkpoint(records.checkpointer, str(self._largest_seq[0]),
                                self._largest_seq[1])
                self._last_checkpoint_time = time.time()

        except Exception as e:
            logging.info("Encountered an exception while processing records."
                         " Exception was %s" % e)

    def shutdown(self, shutdown: amazon_kclpy.messages.ShutdownInput)-> None:
        """
        Called by a KCLProcess instance to indicate that this record processor
        should shutdown. After this is called, there will be no more calls to
        any other methods of this record processor. As part of the shutdown
        process you must inspect

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
        except:
            pass

if __name__ == "__main__":
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()
