"""Tests for the :mod:`references.controllers.references` module."""

import unittest
from unittest import mock
from references.controllers import extracted_references
from moto import mock_dynamodb2
from references.services import data_store
from references import status
from urllib import parse

import logging
for name in ['botocore.endpoint', 'botocore.hooks', 'botocore.auth',
             'botocore.credentials', 'botocore.client',
             'botocore.retryhandler', 'botocore.parsers', 'botocore.waiter',
             'botocore.args']:
    logger = logging.getLogger(name)
    logger.setLevel('ERROR')


class TestReferenceResolver(unittest.TestCase):
    """Test the behavior of :func:`.reference.resolve`."""

    @mock.patch('references.controllers.extracted_references.get')
    def test_resolve_with_arxiv_id(self, mock_get):
        """When arXiv ID is available, returns a redirect to arXiv.org."""
        data = {'identifiers': [{'identifier_type': 'arxiv',
                                 'identifier': '1234.5678'},
                                {'identifier_type': 'isbn',
                                 'identifier': '9999999999999'}],
                'doi': '10.12345/5444',
                'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        mock_get.return_value = (data, 200)
        target_url, status_code = extracted_references.resolve('5432.6789',
                                                               'asdf1234')
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertTrue(target_url.startswith('https://arxiv.org/abs'))

    @mock.patch('references.controllers.extracted_references.get')
    def test_resolve_with_doi(self, mock_get):
        """When DOI is available, returns a redirect to dx.doi.org."""
        data = {'identifiers': [{'identifier_type': 'isbn',
                                 'identifier': '9999999999999'}],
                'doi': '10.12345/5444',
                'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        mock_get.return_value = (data, 200)
        target_url, status_code = extracted_references.resolve('5432.6789',
                                                               'asdf1234')
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertTrue(target_url.startswith('https://dx.doi.org/'))

    @mock.patch('references.controllers.extracted_references.get')
    def test_resolve_with_isbn(self, mock_get):
        """When ISBN is available, returns a redirect to worldcat."""
        data = {'identifiers': [{'identifier_type': 'isbn',
                                 'identifier': '9999999999999'}],
                'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        mock_get.return_value = (data, 200)
        target_url, status_code = extracted_references.resolve('5432.6789',
                                                               'asdf1234')
        self.assertEqual(status_code, status.HTTP_303_SEE_OTHER)
        self.assertTrue(target_url.startswith('https://www.worldcat.org/'))

    @mock.patch('references.controllers.extracted_references.get')
    def test_resolve_with_meta(self, mock_get):
        """If no identifier is available, redirects to Google Scholar."""
        data = {'title': 'Camelot',
                'year': 1999,
                'source': 'Journal of Bardistry',
                'authors': [{'forename': 'Sir', 'surname': 'Robin'}]}
        mock_get.return_value = (data, 200)
        target_url, status_code = extracted_references.resolve('5432.6789',
                                                               'asdf1234')
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

    @mock.patch.object(data_store, 'get_latest_extractions')
    def test_list_calls_datastore_session(self, retrieve_mock):
        """Test :func:`.reference.list` function."""
        retrieve_mock.return_value = {'references': []}
        try:
            response, code = extracted_references.list('arxiv:1234.5678')
        except TypeError:
            self.fail('list() should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)

    @mock.patch.object(data_store, 'get_latest_extractions')
    def test_list_calls_datastore_with_reftype(self, retrieve_mock):
        """Test calls with reftype argument."""
        retrieve_mock.return_value = {'references': []}
        try:
            response, code = extracted_references.list('arxiv:1234.5678',
                                                       'citation')
        except TypeError:
            self.fail('list() should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)
        try:
            retrieve_mock.assert_called_with('arxiv:1234.5678',
                                             reftype='citation')
        except AssertionError as e:
            self.fail('%s' % e)

    @mock.patch.object(data_store, 'get_latest_extractions')
    def test_list_handles_IOError(self, retrieve_mock):
        """Test the case that the underlying datastore raises an IOError."""
        def raise_ioerror(*args, **kwargs):
            raise IOError('Whoops!')
        retrieve_mock.side_effect = raise_ioerror
        try:
            response, code = extracted_references.list('arxiv:1234.5678')
        except TypeError:
            self.fail('list() should return a tuple')
        self.assertEqual(code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    @mock.patch.object(data_store, 'get_latest_extractions')
    def test_list_handles_nonexistant_record(self, retrieve_mock):
        """Test the case that a non-existant record is requested."""
        retrieve_mock.return_value = None
        try:
            response, code = extracted_references.list('arxiv:1234.5678')
        except TypeError:
            self.fail('list() should return a tuple')
        self.assertEqual(code, status.HTTP_404_NOT_FOUND)


class TestReferenceMetadataControllerGet(unittest.TestCase):
    """Test the :func:`.reference.get` function."""

    @mock.patch.object(data_store, 'get_reference')
    def test_get_calls_datastore_session(self, retrieve_mock):
        """Test :func:`.reference.get` function."""
        retrieve_mock.return_value = {}
        try:
            response, code = extracted_references.get('arxiv:1234.5678',
                                                      'asdf')
        except TypeError:
            self.fail('get() should return a tuple')
        self.assertEqual(retrieve_mock.call_count, 1)

    @mock.patch.object(data_store, 'get_reference')
    def test_get_handles_IOError(self, retrieve_mock):
        """The underlying datastore raises an IOError."""
        def raise_ioerror(*args):
            raise IOError('Whoops!')
        retrieve_mock.side_effect = raise_ioerror
        try:
            response, code = extracted_references.get('arxiv:1234.5678',
                                                      'asdf')
        except TypeError:
            self.fail('get() should return a tuple')
        self.assertEqual(code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    @mock.patch.object(data_store, 'get_reference')
    def test_get_handles_nonexistant_record(self, retrieve_mock):
        """A non-existant record is requested."""
        retrieve_mock.return_value = None
        try:
            response, code = extracted_references.get('arxiv:1234.5678',
                                                      'asdf')
        except TypeError:
            self.fail('get() should return a tuple')
        self.assertEqual(code, status.HTTP_404_NOT_FOUND)
