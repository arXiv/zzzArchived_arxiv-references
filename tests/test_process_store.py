"""Tests for the :mod:`reflink.process.store` module."""

import unittest
from unittest import mock
from moto import mock_s3, mock_dynamodb2
from tempfile import mkstemp

from reflink.process.store import store_pdf
from reflink.process.store import store_metadata
from reflink.services import object_store, data_store


class TestStorePDF(unittest.TestCase):
    """Test that :func:`.store_pdf` stores a PDF."""

    def setUp(self):
        """Given a document ID and a path to a PDF on the filesystem..."""
        self.doc_id = 'arxiv:1234.5678'
        _, self.fpath = mkstemp()

    @mock.patch('reflink.services.object_store.get_session')
    def test_store_pdf(self, mock_get_session):
        """Test that :func:`.store_pdf` creates an item in the object store."""

        mock_session = mock.MagicMock()
        mock_get_session.return_value = mock_session

        store_pdf(self.fpath, self.doc_id)

        self.assertEqual(mock_session.create.call_count, 1)
        try:
            mock_session.create.assert_called_with(self.doc_id, self.fpath)
        except AssertionError as e:
            self.fail(str(e))


class TestStoreMetadata(unittest.TestCase):
    """Test that :func:`.store_metadata` stores references."""

    def setUp(self):
        """Given a document ID, references, and an extractor version..."""
        self.doc_id = 'arxiv:1234.5678'
        self.data = [{'foo': 'bar'}]
        self.version = '0.1'

    @mock.patch('reflink.services.data_store.get_session')
    def test_store_metadata(self, mock_get_session):
        """
        Test that :func:`.store_metadata` creates a record in the datastore.
        """
        mock_session = mock.MagicMock()
        mock_get_session.return_value = mock_session

        store_metadata(self.data, self.doc_id, self.version)
        self.assertEqual(mock_session.create.call_count, 1)
        try:
            mock_session.create.assert_called_with(self.doc_id, self.data,
                                                   self.version)
        except AssertionError as e:
            self.fail(str(e))
