"""
This module is used by the KCL MultiLangDaemon to process Kinesis streams.

http://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-implementation-app-py.html
https://github.com/awslabs/amazon-kinesis-client-python/blob/master/samples/sample_kclpy_app.py
"""

import time
import json
import os

from arxiv.base import logging
from arxiv.base.agent import BaseConsumer
from references.process import tasks

from arxiv.base.globals import get_application_config

logger = logging.getLogger(__name__)
logger.propagate = False

# TODO: make configurable.
ARXIV_HOME = 'https://arxiv.org'


class ExtractionAgent(BaseConsumer):
    """Processes records received by the Kinesis consumer."""

    def process_record(self, record: dict) -> None:
        """
        Called for each record on the stream.

        Parameters
        ----------
        record : dict

        """
        # The data payload should be a JSON document.
        try:
            deserialized = json.loads(record['Data'].decode('utf-8'))
        except json.decoder.JSONDecodeError as e:
            logger.error("Error while deserializing data %s", e)
            logger.error("Data payload: %s", record['Data'])
            raise RuntimeError("Could not decode record data")

        # The one required field is ``document_id``. We could consider
        # TODO: supporting arbitrary URIs.
        document_id = deserialized.get('document_id')
        if not document_id:
            logger.error('Record did not contain document ID')
            raise RuntimeError('Record did not contain document ID')
        try:
            # TODO: consider using urljoin here.
            pdf_url = '%s/pdf/%s' % (ARXIV_HOME, document_id)
            tasks.process_document(document_id, pdf_url)
        except Exception as e:
            logger.error('%s: failed to extract references: %s',
                         document_id, e)
            raise RuntimeError('%s: failed to extract references: %s' %
                               (document_id, e)) from e
        logger.info('%s: successfully extracted references', document_id)
        return
