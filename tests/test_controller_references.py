"""Tests for the :mod:`reflink.web.references` module."""

import unittest
from unittest import mock
from reflink.web.references import ReferenceMetadataController
from moto import mock_dynamodb2
from reflink.services import data_store
from reflink import status
from urllib import parse

import logging
for name in ['botocore.endpoint', 'botocore.hooks', 'botocore.auth',
             'botocore.credentials', 'botocore.client',
             'botocore.retryhandler', 'botocore.parsers', 'botocore.waiter',
             'botocore.args']:
    logger = logging.getLogger(name)
    logger.setLevel('ERROR')


class TestReferenceResolver(unittest.TestCase):
    """Test the behavior of :meth:`.ReferenceMetadataController.resolve`."""

    def test_resolve_with_arxiv_id(self):
        """When arXiv ID is available, returns a redirect to arXiv.org."""
        controller = ReferenceMetadataController()
        data = {'identifiers': [{'identifier_type': 'arxiv',
                                 'identifier': '1234.5678'},
                                {'identifier_type': 'isbn',
                                 'identifier': '9999999999999'}],
                'doi': '10.12345/5444',
                'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        type(controller).get = mock.MagicMock(return_value=(data, 200))
        target_url, status_code = controller.resolve('5432.6789', 'asdf1234')
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertTrue(target_url.startswith('https://arxiv.org/abs'))

    def test_resolve_with_doi(self):
        """When DOI is available, returns a redirect to dx.doi.org."""
        controller = ReferenceMetadataController()
        data = {'identifiers': [{'identifier_type': 'isbn',
                                 'identifier': '9999999999999'}],
                'doi': '10.12345/5444',
                'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        type(controller).get = mock.MagicMock(return_value=(data, 200))
        target_url, status_code = controller.resolve('5432.6789', 'asdf1234')
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertTrue(target_url.startswith('https://dx.doi.org/'))

    def test_resolve_with_isbn(self):
        """When ISBN is available, returns a redirect to worldcat."""
        controller = ReferenceMetadataController()
        data = {'identifiers': [{'identifier_type': 'isbn',
                                 'identifier': '9999999999999'}],
                'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        type(controller).get = mock.MagicMock(return_value=(data, 200))
        target_url, status_code = controller.resolve('5432.6789', 'asdf1234')
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertTrue(target_url.startswith('https://www.worldcat.org/'))

    def test_resolve_with_meta(self):
        """If no identifier is available, redirects to Google Scholar."""
        controller = ReferenceMetadataController()
        data = {'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        type(controller).get = mock.MagicMock(return_value=(data, 200))
        target_url, status_code = controller.resolve('5432.6789', 'asdf1234')
        url = parse.urlparse(target_url)
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertEqual(url.netloc, 'scholar.google.com')
        self.assertEqual(url.path, '/scholar')
        query = parse.parse_qs(url.query)
        self.assertEqual(query['as_q'], ['Camelot'])
        self.assertEqual(query['as_sauthors'], ['"Sir Robin"'])
        self.assertEqual(query['as_yhi'], ['1999'])
        self.assertEqual(query['as_ylo'], ['1999'])
        self.assertEqual(query['as_publication'], ['Journal of Bardistry'])


class TestReferenceMetadataControllerList(unittest.TestCase):
    """Test the ReferenceMetadataController.list method."""

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve_latest')
    def test_list_calls_datastore_session(self, retrieve_mock):
        """Test ReferenceMetadataController.list method."""
        retrieve_mock.return_value = []
        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.list('arxiv:1234.5678')
        except TypeError:
            self.fail('ReferenceMetadataController.list should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve_latest')
    def test_list_calls_datastore_with_reftype(self, retrieve_mock):
        """Test calls with reftype argument."""
        retrieve_mock.return_value = []
        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.list('arxiv:1234.5678',
                                                    'citation')
        except TypeError:
            self.fail('ReferenceMetadataController.list should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)
        try:
            retrieve_mock.assert_called_with('arxiv:1234.5678',
                                             reftype='citation')
        except AssertionError as e:
            self.fail('%s' % e)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve_latest')
    def test_list_handles_IOError(self, retrieve_mock):
        """Test the case that the underlying datastore raises an IOError."""
        def raise_ioerror(*args, **kwargs):
            raise IOError('Whoops!')
        retrieve_mock.side_effect = raise_ioerror

        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.list('arxiv:1234.5678')
        except TypeError:
            self.fail('ReferenceMetadataController.list should return a tuple')

        self.assertEqual(status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve_latest')
    def test_list_handles_nonexistant_record(self, retrieve_mock):
        """Test the case that a non-existant record is requested."""
        retrieve_mock.return_value = None

        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.list('arxiv:1234.5678')
        except TypeError:
            self.fail('ReferenceMetadataController.list should return a tuple')

        self.assertEqual(status_code, status.HTTP_404_NOT_FOUND)


class TestReferenceMetadataControllerGet(unittest.TestCase):
    """Test the ReferenceMetadataController.get method."""

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve')
    def test_get_calls_datastore_session(self, retrieve_mock):
        """Test ReferenceMetadataController.get method."""
        retrieve_mock.return_value = {}
        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.get('arxiv:1234.5678', 'asdf')
        except TypeError:
            self.fail('ReferenceMetadataController.get should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve')
    def test_get_handles_IOError(self, retrieve_mock):
        """Test the case that the underlying datastore raises an IOError."""
        def raise_ioerror(*args):
            raise IOError('Whoops!')
        retrieve_mock.side_effect = raise_ioerror

        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.get('arxiv:1234.5678', 'asdf')
        except TypeError:
            self.fail('ReferenceMetadataController.get should return a tuple')

        self.assertEqual(status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    @mock_dynamodb2
    @mock.patch.object(data_store.ReferenceStoreSession, 'retrieve')
    def test_get_handles_nonexistant_record(self, retrieve_mock):
        """Test the case that a non-existant record is requested."""
        retrieve_mock.return_value = None

        controller = ReferenceMetadataController()

        try:
            response, status_code = controller.get('arxiv:1234.5678', 'asdf')
        except TypeError:
            self.fail('ReferenceMetadataController.get should return a tuple')

        self.assertEqual(status_code, status.HTTP_404_NOT_FOUND)
