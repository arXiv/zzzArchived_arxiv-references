"""Service layer for link-injected PDF object storage."""

import boto3
from botocore.exceptions import ClientError
import os

from flask import _app_ctx_stack as stack
from reflink import logging

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
        logger.info('Store PDF for %s: %s' % (document_id, filepath))
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
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf1234')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa5678')

    return PDFStoreSession(bucket_name, endpoint_url, aws_access_key,
                           aws_secret_key)


class ObjectStore(object):
    """Object store service integration from reflink Flask application."""
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('REFLINK_S3_BUCKET', 'arxiv-reflink')
        app.config.setdefault('REFLINK_S3_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'asdf1234')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'fdsa5678')
        app.config.setdefault('REFLINK_AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            bucket_name = self.app.config['REFLINK_S3_BUCKET']
            endpoint_url = self.app.config['REFLINK_S3_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['REFLINK_AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No application context.
            bucket_name = os.environ.get('REFLINK_S3_BUCKET', 'arxiv-reflink')
            endpoint_url = os.environ.get('REFLINK_S3_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
        return PDFStoreSession(bucket_name, endpoint_url, aws_access_key,
                               aws_secret_key)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'object_store'):
                ctx.object_store = self.get_session()
            return ctx.object_store
        return self.get_session()     # No application context.
