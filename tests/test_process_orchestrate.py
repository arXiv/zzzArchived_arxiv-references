"""Tests for the :mod:`reflink.process.orchestrate` module."""

import unittest
from moto import mock_s3, mock_dynamodb2
from tempfile import mkstemp

from reflink.process.store import store_pdf
from reflink.process.store import store_metadata
from reflink.services import object_store, data_store


class TestStore(unittest.TestCase):
    """Test tasks (functions) in the :mod:`reflink.process.store` module."""

    @mock_s3
    def test_store_pdf(self):
        """
        Test the :func:`reflink.process.store.store_pdf` task (function).

        The task :func:`reflink.process.store.store_pdf` should generate a new
        object in the object store.
        """
        document_id = 'arxiv:1234.5678'

        _, fpath = mkstemp()
        store_pdf(fpath, document_id)

        objects = object_store.get_session()
        url = objects.retrieve_url(document_id)
        self.assertIsInstance(url, str)
        self.assertTrue(url.endswith(document_id))

    @mock_dynamodb2
    def test_store_metadata(self):
        """
        Test the :func:`reflink.process.store.store_metadata` task (function).

        Should generate a new record in the datastore.
        """
        document_id = 'arxiv:1234.5678'
        store_metadata({'foo': 'bar'}, document_id)

        store = data_store.get_session()
        data = store.retrieve(document_id)
        self.assertIsInstance(data, dict)
        self.assertEqual(data['foo'], 'bar')
