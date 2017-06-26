import sys
sys.path.append('.')

import unittest
from unittest import mock
from reflink.web.controllers import references
from moto import mock_dynamodb2
from reflink.services import data_store
from reflink import status


class TestReferenceMetadataController(unittest.TestCase):
    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve')
    def test_get_calls_datastore_session(self, retrieve_mock):
        retrieve_mock.return_value = []
        controller = references.ReferenceMetadataController()

        try:
            response, status_code = controller.get('arxiv:1234.5678')
        except TypeError:
            self.fail('ReferenceMetadataController.get should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve')
    def test_get_handles_IOError(self, retrieve_mock):
        def raise_ioerror(*args):
            raise IOError('Whoops!')
        retrieve_mock.side_effect = raise_ioerror

        controller = references.ReferenceMetadataController()

        try:
            response, status_code = controller.get('arxiv:1234.5678')
        except TypeError:
            self.fail('ReferenceMetadataController.get should return a tuple')

        self.assertEqual(status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve')
    def test_get_handles_nonexistant_record(self, retrieve_mock):
        retrieve_mock.return_value = None

        controller = references.ReferenceMetadataController()

        try:
            response, status_code = controller.get('arxiv:1234.5678')
        except TypeError:
            self.fail('ReferenceMetadataController.get should return a tuple')

        self.assertEqual(status_code, status.HTTP_404_NOT_FOUND)


if __name__ == '__main__':
    unittest.main()
