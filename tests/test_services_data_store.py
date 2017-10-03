"""Tests for the :mod:`references.services.data_store` module."""

import unittest
import os
from moto import mock_dynamodb2

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


class StoreReference(unittest.TestCase):
    """The data store should store a reference."""

    @mock_dynamodb2
    def test_invalid_data_raises_valueerror(self):
        """A ValueError is raised when invalid data are passed."""
        invalid_data = [{"foo": "bar", "baz": 347}]

        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        with self.assertRaises(ValueError):
            session.create(document_id, invalid_data, version)

    @mock_dynamodb2
    def test_valid_data_are_stored(self):
        """Valid data are inserted into the datastore."""
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        extraction, data = session.create(document_id, valid_data, version)

        # Get the data that we just inserted.
        retrieved = session.retrieve_all(document_id, extraction)
        self.assertEqual(len(data), len(retrieved))
        self.assertEqual(len(valid_data), len(retrieved))


class RetrieveReference(unittest.TestCase):
    """Test retrieving data from the datastore."""

    @mock_dynamodb2
    def test_retrieve_by_arxiv_id_and_extraction(self):
        """
        Data for a specific document are retrieved from the datastore.

        After a set of references are saved, we should be able to retrieve
        those references using the arXiv identifier and extraction id.
        """
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        extraction, data = session.create(document_id, valid_data, version)

        retrieved = session.retrieve_all(document_id, extraction)
        self.assertEqual(len(data), len(valid_data))

        # Order should be preserved.
        for original, returned, final in zip(valid_data, data, retrieved):
            self.assertEqual(original['raw'], returned['raw'])
            self.assertEqual(original['raw'], final['raw'])

    @mock_dynamodb2
    def test_retrieve_specific_reftype(self):
        """Retrieve only references of a specific reftype."""
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        data = valid_data + valid_href_data
        extraction, data = session.create(document_id, data, version)

        citations = session.retrieve_all(document_id, extraction, 'citation')
        self.assertEqual(len(citations), len(valid_data))

        hrefs = session.retrieve_all(document_id, extraction, 'href')
        self.assertEqual(len(hrefs), len(valid_href_data))
        self.assertEqual(valid_href_data[0]['href'], hrefs[0]['href'])

    @mock_dynamodb2
    def test_retrieve_latest_by_arxiv_id(self):
        """
        Retrieve data for the latest extraction from a document.

        After a set of references are saved, we should be able to retrieve
        the latest references using the arXiv identifier.
        """
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        first_extraction, _ = session.create(document_id, valid_data, version)
        new_version = '0.2'
        second_extraction, _ = session.create(document_id, valid_data[::-1],
                                              new_version)

        data = session.retrieve_latest(document_id)
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
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        extraction, data = session.create(document_id, valid_data, version)
        identifier = data[0]['identifier']
        retrieved = session.retrieve(document_id, identifier)
        self.assertEqual(data[0]['raw'], retrieved['raw'])

    @mock_dynamodb2
    def test_retrieving_nonexistant_record_returns_none(self):
        """Return ``None`` when references for a paper do not exist."""
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        data = session.retrieve_latest('nonsense')
        self.assertEqual(data, None)

    @mock_dynamodb2
    def test_retrieving_nonexistant_reference_returns_none(self):
        """Return ``None`` when an extraction does not exist."""
        session = data_store.referencesStore.session
        session.create_table()
        session.extractions.create_table()
        data = session.retrieve('nonsense', 'gibberish')
        self.assertEqual(data, None)


class TestPreSaveDataIsMessy(unittest.TestCase):
    def test_reference_is_none(self):
        """A NoneType object slipped into the reference metadata."""
        try:
            self.assertEqual(data_store.ReferenceStoreSession._clean(None),
                             None)
        except Exception as E:
            self.fail('NoneType objects should be handled gracefully')

    def test_reference_contains_a_none(self):
        """A value in the reference is a NoneType object."""
        try:
            ref = data_store.ReferenceStoreSession._clean({
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
            ref = data_store.ReferenceStoreSession._clean({
                'foo': 'bar',
                'baz': ['bat', None]
            })
            self.assertDictEqual(ref, {'foo': 'bar', 'baz': ['bat']},
                                 "The null value should be dropped.")
        except Exception as E:
            self.fail('NoneType objects should be handled gracefully')


if __name__ == '__main__':
    unittest.main()
