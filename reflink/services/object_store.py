"""Service layer for link-injected PDF object storage."""

import logging
import boto3
from botocore.exceptions import ClientError
import os

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format,
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PDFStoreSession(object):
    """
    Provides a CRUD interface for the PDF object store.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket in which to deposit PDFs.
    endpoint_url : str
        If ``None``, uses AWS defaults. Mostly useful for local development
        and testing.
    aws_access_key : str
    aws_secret_key : str
    """

    URI = 'https://{bucket_name}.s3.amazonaws.com/{document_id}'

    def __init__(self, bucket_name: str, endpoint_url: str,
                 aws_access_key: str, aws_secret_key: str) -> None:
        """Establish an S3 client, and ensure that the bucket exists."""
        self.s3 = boto3.client('s3', endpoint_url=endpoint_url,
                               aws_access_key_id=aws_access_key,
                               aws_secret_access_key=aws_secret_key)
        self.bucket_name = bucket_name
        try:    # Does the bucket exist? If not, create it.
            self.s3.get_bucket_location(Bucket=self.bucket_name)
        except ClientError as e:
            if 403 == e.response['ResponseMetadata']['HTTPStatusCode']:
                logger.error('AWS S3 access denied; aborting.')
                raise IOError('AWS S3 access denied; aborting.') from e
            logger.info('Bucket %s does not exist' % self.bucket_name)
            self._create_bucket()

    def _create_bucket(self) -> None:
        """Create a new bucket."""
        logger.info('Attempting to create bucket %s' % self.bucket_name)
        try:
            self.s3.create_bucket(ACL='public-read', Bucket=self.bucket_name)
        except ClientError as e:
            if 403 == e.response['ResponseMetadata']['HTTPStatusCode']:
                raise IOError('AWS S3 access denied; aborting.') from e
            raise IOError('Could not create bucket: %s' % e) from e

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
        fname = '%s.pdf' % document_id
        try:
            with open(filepath, 'rb') as f:   # Must produce binary when read.
                self.s3.upload_fileobj(f, self.bucket_name, fname)
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
        fname = '%s.pdf' % document_id
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=fname)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':    # Object doesn't exist.
                return None
            raise IOError('Failed to retrieve URL: %s' % e) from e
        return self.URI.format(bucket_name=self.bucket_name,
                               document_id=fname)


def get_session() -> PDFStoreSession:
    """
    Get a new PDF store session.

    Returns
    -------
    :class:`.PDFStoreSession`
    """
    bucket_name = os.environ.get('REFLINK_S3_BUCKET', 'arxiv-reflink')
    endpoint_url = os.environ.get('REFLINK_S3_ENDPOINT', None)
    aws_access_key = os.environ.get('AWS_ACCESS_KEY', 'asdf1234')
    aws_secret_key = os.environ.get('AWS_SECRET_KEY', 'fdsa5678')

    return PDFStoreSession(bucket_name, endpoint_url, aws_access_key,
                           aws_secret_key)
