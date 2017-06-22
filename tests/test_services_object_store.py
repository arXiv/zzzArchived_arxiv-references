import sys
sys.path.append('.')


import unittest
from moto import mock_s3
import boto3
import os
from reflink.services import object_store




class StorePDF(unittest.TestCase):
    """
    The object store should store a PDF.
    """

    @mock_s3
    def test_object_is_stored(self):
        """
        If the data is valid, it should be inserted into the database.
        """

        document_id = 'arxiv:1234.5678'
        filepath = 'tests/data/1702.07336.pdf'
        bucket_name = "test"
        os.environ.setdefault('REFLINK_S3_BUCKET', bucket_name)

        session = object_store.get_session()
        session.create(document_id, filepath)

        # Get the object that we just inserted.
        s3 = boto3.client('s3')
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': document_id
            }
        )
        self.assertIsInstance(url, str)
        self.assertTrue(url.startswith('https://%s.s3.amazonaws.com/' % bucket_name))



class RetrieveObjectURL(unittest.TestCase):
    """
    The object store should retrieve presigned URLs..
    """

    @mock_s3
    def test_retrieve_by_arxiv_id(self):
        document_id = 'arxiv:1234.5678'
        filepath = 'tests/data/1702.07336.pdf'
        bucket_name = "test"
        os.environ.setdefault('REFLINK_S3_BUCKET', bucket_name)

        session = object_store.get_session()
        session.create(document_id, filepath)

        url = session.retrieve_url(document_id)
        self.assertIsInstance(url, str)
        self.assertTrue(url.startswith('https://%s.s3.amazonaws.com/' % bucket_name))

    @mock_s3
    def test_retrieving_nonexistant_record_returns_none(self):
        """
        If the record does not exist, attempting to retrieve it should simply
        return ``None``.
        """
        document_id = 'arxiv:1234.5678'
        session = object_store.get_session()
        url = session.retrieve_url(document_id)
        self.assertEqual(url, None)


if __name__ == '__main__':
    unittest.main()
