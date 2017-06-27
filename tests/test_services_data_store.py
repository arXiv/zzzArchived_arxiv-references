"""Tests for the :mod:`reflink.services.data_store` module."""
import unittest
import boto3
import os
from moto import mock_dynamodb2

from reflink.services import data_store


schema_path = 'schema/references.json'


class StoreReference(unittest.TestCase):
    """The data store should store a reference."""

    @mock_dynamodb2
    def test_invalid_data_raises_valueerror(self):
        """
        Test the case that invalid data are passed to the datastore.

        If the input data does not conform to the JSON schema, we should raise
        a ValueError.
        """
        invalid_data = [{"foo": "bar", "baz": 347}]
        document_id = 'arxiv:1234.5678'
        os.environ.setdefault('REFLINK_SCHEMA', schema_path)

        session = data_store.get_session()
        with self.assertRaises(ValueError):
            session.create(document_id, invalid_data)

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('ReferenceSet')
        table.get_item(Key={'document': document_id})

    @mock_dynamodb2
    def test_valid_data_is_stored(self):
        """
        Test the case that valid data are passed to the datastore.

        If the data is valid, it should be inserted into the database.
        """
        valid_data = [
            {
                "raw": "Peirson et al 2015 blah blah",
                "reftype": "journalArticle"
            }
        ]
        document_id = 'arxiv:1234.5678'
        os.environ.setdefault('REFLINK_SCHEMA', schema_path)

        session = data_store.get_session()
        session.create(document_id, valid_data)

        # Get the data that we just inserted.
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('ReferenceSet')
        item = table.get_item(Key={'document': document_id})
        self.assertIsInstance(item, dict)
        self.assertEqual(item['ResponseMetadata']['HTTPStatusCode'], 200)


class RetrieveReference(unittest.TestCase):
    """Test retrieving data from the datastore."""

    @mock_dynamodb2
    def test_retrieve_by_arxiv_id(self):
        """
        Test retrieving data from the datastore.

        After a set of references are saved, we should be able to retrieve
        those references using the arXiv identifier.
        """
        valid_data = [
            {
                "raw": "Peirson et al 2015 blah blah",
                "reftype": "journalArticle"
            }
        ]
        document_id = 'arxiv:1234.5678'
        os.environ.setdefault('REFLINK_SCHEMA', schema_path)

        session = data_store.get_session()
        session.create(document_id, valid_data)

        data = session.retrieve(document_id)
        # self.assertEqual(data['document'], document_id)
        self.assertEqual(data[0]['raw'], valid_data[0]['raw'])
        self.assertEqual(data[0]['reftype'], valid_data[0]['reftype'])

    @mock_dynamodb2
    def test_retrieving_nonexistant_record_returns_none(self):
        """
        Test retrieving a record that does not exist.

        If the record does not exist, attempting to retrieve it should simply
        return ``None``.
        """
        document_id = 'arxiv:1234.5678'
        os.environ.setdefault('REFLINK_SCHEMA', schema_path)

        session = data_store.get_session()
        data = session.retrieve(document_id)
        self.assertEqual(data, None)


if __name__ == '__main__':
    unittest.main()
