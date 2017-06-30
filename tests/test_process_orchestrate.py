"""Tests for the :mod:`reflink.process.orchestrate` module."""

import unittest
from unittest import mock
from moto import mock_s3, mock_dynamodb2
from tempfile import mkstemp

from reflink.process.store import store_pdf
from reflink.process.store import store_metadata
from reflink.services import object_store, data_store


class TestStore(unittest.TestCase):
    """Test tasks (functions) in the :mod:`reflink.process.store` module."""

    @mock.patch('reflink.services.object_store.get_session')
    def test_store_pdf(self, mock_get_session):
        """
        Test the :func:`reflink.process.store.store_pdf` task (function).

        The task :func:`reflink.process.store.store_pdf` should generate a new
        object in the object store.
        """
        document_id = 'arxiv:1234.5678'

        mock_session = mock.MagicMock()
        mock_get_session.return_value = mock_session

        _, fpath = mkstemp()
        store_pdf(fpath, document_id)

        self.assertEqual(mock_session.create.call_count, 1)
        try:
            mock_session.create.assert_called_with(document_id, fpath)
        except AssertionError as e:
            self.fail(str(e))

    @mock.patch('reflink.services.data_store.get_session')
    def test_store_metadata(self, mock_get_session):
        """
        Test the :func:`reflink.process.store.store_metadata` task (function).

        Should generate a new record in the datastore.
        """
        mock_session = mock.MagicMock()
        mock_get_session.return_value = mock_session

        document_id = 'arxiv:1234.5678'
        data = [{'foo': 'bar'}]
        store_metadata(data, document_id)
        self.assertEqual(mock_session.create.call_count, 1)
        try:
            mock_session.create.assert_called_with(document_id, data)
        except AssertionError as e:
            self.fail(str(e))
