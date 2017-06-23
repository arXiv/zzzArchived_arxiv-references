import boto3
from botocore.exceptions import ClientError
import os


class PDFStoreSession(object):
    URI = 'https://{bucket_name}.s3.amazonaws.com/{document_id}'

    def __init__(self, bucket_name: str, endpoint_url: str,
                 aws_access_key: str, aws_secret_key: str) -> None:
        self.s3 = boto3.client('s3', endpoint_url=endpoint_url,
                               aws_access_key_id=aws_access_key,
                               aws_secret_access_key=aws_secret_key)
        self.bucket_name = bucket_name
        try:    # Does the bucket exist? If not, create it.
            self.s3.get_bucket_location(Bucket=self.bucket_name)
        except ClientError:
            self._create_bucket()

    def _create_bucket(self):
        """
        Create a new bucket.
        """
        # TODO: make this configurable.
        self.s3.create_bucket(ACL='public-read', Bucket=self.bucket_name)

    def create(self, document_id: str, filepath: str) -> str:
        """
        Store a PDF for a document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.56789``.
        filepath : str
            Full path to file on the local filesystem.

        Raises
        ------
        IOError
            Raised when there is a problem sending the file to S3.
        """

        try:
            with open(filepath, 'rb') as f:   # Must produce binary when read.
                self.s3.upload_fileobj(f, self.bucket_name, document_id)
        except ClientError as e:
            raise IOError('Failed to upload: %s' % e)

    def retrieve_url(self, document_id: str) -> str:
        """
        Obtain an URL of the PDF for a document.

        Parameters
        ----------
        document_id : str
            Document identifier, e.g. ``arxiv:1234.56789``.

        Returns
        -------
        str
            URL for PDF, e.g. to provide to a client.
        """
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=document_id)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':    # Object doesn't exist.
                return None
            raise IOError('Failed to retrieve URL: %s' % e)
        return self.URI.format(bucket_name=self.bucket_name,
                               document_id=document_id)


def get_session() -> PDFStoreSession:
    """
    Get a new PDF store session.


    Returns
    -------
    :class:`.PDFStoreSession`
    """
    bucket_name = os.environ.get('REFLINK_S3_BUCKET', 'arxiv-reflink')
    endpoint_url = os.environ.get('REFLINK_S3_ENDPOINT', None)
    aws_access_key = os.environ.get('REFLINK_AWS_ACCESS_KEY', 'asdf1234')
    aws_secret_key = os.environ.get('REFLINK_AWS_SECRET_KEY', 'fdsa5678')

    return PDFStoreSession(bucket_name, endpoint_url, aws_access_key,
                           aws_secret_key)
