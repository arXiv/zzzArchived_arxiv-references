"""Tests for the :mod:`references.services.data_store` module."""

import unittest
from unittest import mock
import os
from moto import mock_dynamodb2
import dateutil.parser
import datetime
from references.services import data_store
import logging
for name in ['botocore.endpoint', 'botocore.hooks', 'botocore.auth',
             'botocore.credentials', 'botocore.client',
             'botocore.retryhandler', 'botocore.parsers', 'botocore.waiter',
             'botocore.args']:
    logger = logging.getLogger(name)
    logger.setLevel('ERROR')


schema_path = 'schema/StoredReference.json'
extracted_path = 'schema/ExtractedReference.json'
dynamodb_endpoint = 'http://localhost:4569'
os.environ.setdefault('REFLINK_STORED_SCHEMA', schema_path)
os.environ.setdefault('REFLINK_EXTRACTED_SCHEMA', extracted_path)
# os.environ.setdefault('DYNAMODB_ENDPOINT', dynamodb_endpoint)


valid_data = [
    {
        "raw": "Peirson et al 2015 blah blah",
        "reftype": "citation"
    },
    {
        "raw": "Jones 2012",
        "reftype": "citation"
    },
    {
        "raw": "Majumdar 1968 etc",
        "reftype": "citation"
    },
    {
        "raw": "The brown fox, 1921",
        "reftype": "citation"
    }
]

valid_href_data = [{
    "raw": "www.asdf.com",
    "href": "http://www.asdf.com",
    "reftype": "href"
}]

document_id = 'arxiv:1234.5678'
version = '0.1'


class StoreAndRetrieveRawExtractions(unittest.TestCase):
    """Raw extraction metadata from individual extractors should be stored."""

    @mock_dynamodb2
    def setUp(self):
        """Initialize the datastore session."""
        default_args = (None, 'nope', 'nope', None, 'us-east-1')
        default_kwargs = {}
        self.session = data_store.RawExtractionSession(*default_args,
                                                       **default_kwargs)

    @mock_dynamodb2
    def test_can_create_and_retrieve(self):
        """Should be able to create and retrieve raw extraction metadata."""
        self.session.create_table()
        before = datetime.datetime.now()
        self.session.store_extraction(document_id, 'baz', valid_data)
        after = datetime.datetime.now()
        raw = self.session.get_extraction(document_id, 'baz')
        self.assertIsInstance(raw, dict)
        self.assertIn('created', raw, "A timestamp is added")
        try:
            created = dateutil.parser.parse(raw['created'])
            self.assertTrue(before < created)
            self.assertTrue(created < after)
        except ValueError:
            self.fail('Timestamp should be interpretable as a datetime')

        self.assertIn('references', raw)
        self.assertEqual(len(raw['references']), 4)


class StoreReference(unittest.TestCase):
    """The data store should store a reference."""

    @mock_dynamodb2
    def setUp(self):
        """Initialize the datastore session."""
        default_args = (None, 'nope', 'nope', None, 'us-east-1')
        default_kwargs = {
            'extracted_schema': extracted_path,
            'stored_schema': schema_path
        }
        self.session = data_store.ReferenceSession(*default_args,
                                                        **default_kwargs)

    @mock_dynamodb2
    def test_invalid_data_raises_valueerror(self):
        """A ValueError is raised when invalid data are passed."""
        invalid_data = [{"foo": "bar", "baz": 347}]
        self.session.create_table()
        self.session.extractions.create_table()

        with self.assertRaises(ValueError):
            self.session.create(document_id, invalid_data, version)

    @mock_dynamodb2
    def test_valid_data_are_stored(self):
        """Valid data are inserted into the datastore."""
        self.session.create_table()
        self.session.extractions.create_table()
        extraction, data = self.session.create(document_id, valid_data,
                                               version)
        # Get the data that we just inserted.
        retrieved = self.session.retrieve_all(document_id, extraction)
        self.assertEqual(len(data), len(retrieved))
        self.assertEqual(len(valid_data), len(retrieved))


class RetrieveReference(unittest.TestCase):
    """Test retrieving data from the datastore."""

    @mock_dynamodb2
    def setUp(self):
        """Initialize the datastore session."""
        default_args = (None, 'nope', 'nope', None, 'us-east-1')
        default_kwargs = {}
        self.session = data_store.ReferenceSession(*default_args,
                                                        **default_kwargs)

    @mock_dynamodb2
    def test_retrieve_by_arxiv_id_and_extraction(self):
        """
        Data for a specific document are retrieved from the datastore.

        After a set of references are saved, we should be able to retrieve
        those references using the arXiv identifier and extraction id.
        """
        self.session.create_table()
        self.session.extractions.create_table()
        ext, data = self.session.create(document_id, valid_data, version)

        retrieved = self.session.retrieve_all(document_id, ext)
        self.assertEqual(len(data), len(valid_data))

        # Order should be preserved.
        for original, returned, final in zip(valid_data, data, retrieved):
            self.assertEqual(original['raw'], returned['raw'])
            self.assertEqual(original['raw'], final['raw'])

    @mock_dynamodb2
    def test_retrieve_specific_reftype(self):
        """Retrieve only references of a specific reftype."""
        self.session.create_table()
        self.session.extractions.create_table()
        data = valid_data + valid_href_data
        extraction, data = self.session.create(document_id, data, version)
        citations = self.session.retrieve_all(document_id, extraction,
                                              'citation')
        self.assertEqual(len(citations), len(valid_data))

        hrefs = self.session.retrieve_all(document_id, extraction, 'href')
        self.assertEqual(len(hrefs), len(valid_href_data))
        self.assertEqual(valid_href_data[0]['href'], hrefs[0]['href'])

    @mock_dynamodb2
    def test_retrieve_latest_by_arxiv_id(self):
        """
        Retrieve data for the latest extraction from a document.

        After a set of references are saved, we should be able to retrieve
        the latest references using the arXiv identifier.
        """
        self.session.create_table()
        self.session.extractions.create_table()
        first_extraction, _ = self.session.create(document_id, valid_data,
                                                  version)
        new_version = '0.2'
        second_extraction, _ = self.session.create(document_id,
                                                   valid_data[::-1],
                                                   new_version)

        data = self.session.retrieve_latest(document_id)
        self.assertEqual(len(data), len(valid_data),
                         "Only one set of references should be retrieved.")

        # Order should be preserved.
        for first, second, final in zip(valid_data, valid_data[::-1], data):
            self.assertNotEqual(first['raw'], final['raw'])
            self.assertEqual(second['raw'], final['raw'])

    @mock_dynamodb2
    def test_retrieve_specific_reference(self):
        """
        Retrieve data for a specific reference in a document.

        After a set of references are saved, we should be able to retrieve
        a specific reference by its document id and identifier.
        """
        self.session.create_table()
        self.session.extractions.create_table()
        _, data = self.session.create(document_id, valid_data, version)
        identifier = data[0]['identifier']
        retrieved = self.session.retrieve(document_id, identifier)
        self.assertEqual(data[0]['raw'], retrieved['raw'])

    @mock_dynamodb2
    def test_retrieving_nonexistant_record_returns_none(self):
        """Return ``None`` when references for a paper do not exist."""
        self.session.create_table()
        self.session.extractions.create_table()
        data = self.session.retrieve_latest('nonsense')
        self.assertEqual(data, None)

    @mock_dynamodb2
    def test_retrieving_nonexistant_reference_returns_none(self):
        """Return ``None`` when an extraction does not exist."""
        self.session.create_table()
        self.session.extractions.create_table()
        data = self.session.retrieve('nonsense', 'gibberish')
        self.assertEqual(data, None)


class TestPreSaveDataIsMessy(unittest.TestCase):
    """In some cases, None-ish and Falsey values will slip through."""

    def test_reference_is_none(self):
        """A NoneType object slipped into the reference metadata."""
        try:
            self.assertEqual(data_store.ReferenceSession._clean(None),
                             None)
        except Exception as E:
            self.fail('NoneType objects should be handled gracefully')

    def test_reference_contains_a_none(self):
        """A value in the reference is a NoneType object."""
        try:
            ref = data_store.ReferenceSession._clean({
                'foo': 'bar',
                'baz': None
            })
            self.assertDictEqual(ref, {'foo': 'bar'},
                                 "The null field should be dropped.")
        except Exception as E:
            self.fail('NoneType objects should be handled gracefully')

    def test_reference_value_contains_a_none(self):
        """A value in the reference contains a NoneType object."""
        try:
            ref = data_store.ReferenceSession._clean({
                'foo': 'bar',
                'baz': ['bat', None]
            })
            self.assertDictEqual(ref, {'foo': 'bar', 'baz': ['bat']},
                                 "The null value should be dropped.")
        except Exception as E:
            self.fail('NoneType objects should be handled gracefully')
