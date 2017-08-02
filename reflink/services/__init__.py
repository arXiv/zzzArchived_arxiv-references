"""This module provides service layers for external state."""

import os

from flask import _app_ctx_stack as stack

from reflink.services import data_store, object_store


class DataStore(object):
    """Data store service integration from reflink Flask application."""
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('REFLINK_EXTRACTED_SCHEMA', None)
        app.config.setdefault('REFLINK_STORED_SCHEMA', None)
        app.config.setdefault('REFLINK_DYNAMODB_ENDPOINT', None)
        app.config.setdefault('REFLINK_AWS_ACCESS_KEY', 'asdf1234')
        app.config.setdefault('REFLINK_AWS_SECRET_KEY', 'fdsa5678')
        app.config.setdefault('REFLINK_AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            extracted_schema_path = self.app.config['REFLINK_EXTRACTED_SCHEMA']
            stored_schema_path = self.app.config['REFLINK_STORED_SCHEMA']
            endpoint_url = self.app.config['REFLINK_DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['REFLINK_AWS_ACCESS_KEY']
            aws_secret_key = self.app.config['REFLINK_AWS_SECRET_KEY']
            region_name = self.app.config['REFLINK_AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No application context.
            extracted_schema_path = os.environ.get('REFLINK_EXTRACTED_SCHEMA',
                                                   None)
            stored_schema_path = os.environ.get('REFLINK_STORED_SCHEMA', None)
            endpoint_url = os.environ.get('REFLINK_DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('REFLINK_AWS_ACCESS_KEY', 'asdf')
            aws_secret_key = os.environ.get('REFLINK_AWS_SECRET_KEY', 'fdsa')
            region_name = os.environ.get('REFLINK_AWS_REGION', 'us-east-1')
        return data_store.ReferenceStoreSession(endpoint_url,
                                                extracted_schema_path,
                                                stored_schema_path,
                                                aws_access_key, aws_secret_key,
                                                region_name)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'data_store'):
                ctx.data_store = self.get_session()
            return ctx.data_store
        return self.get_session()     # No application context.



class ObjectStore(object):
    """Object store service integration from reflink Flask application."""
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('REFLINK_S3_BUCKET', 'arxiv-reflink')
        app.config.setdefault('REFLINK_S3_ENDPOINT', None)
        app.config.setdefault('REFLINK_AWS_ACCESS_KEY', 'asdf1234')
        app.config.setdefault('REFLINK_AWS_SECRET_KEY', 'fdsa5678')
        app.config.setdefault('REFLINK_AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            bucket_name = self.app.config['REFLINK_S3_BUCKET']
            endpoint_url = self.app.config['REFLINK_S3_ENDPOINT']
            aws_access_key = self.app.config['REFLINK_AWS_ACCESS_KEY']
            aws_secret_key = self.app.config['REFLINK_AWS_SECRET_KEY']
            region_name = self.app.config['REFLINK_AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No application context.
            bucket_name = os.environ.get('REFLINK_S3_BUCKET', 'arxiv-reflink')
            endpoint_url = os.environ.get('REFLINK_S3_ENDPOINT', None)
            aws_access_key = os.environ.get('REFLINK_AWS_ACCESS_KEY', 'asdf')
            aws_secret_key = os.environ.get('REFLINK_AWS_SECRET_KEY', 'fdsa')
        return object_store.PDFStoreSession(bucket_name, endpoint_url,
                                            aws_access_key, aws_secret_key)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'object_store'):
                ctx.object_store = self.get_session()
            return ctx.object_store
        return self.get_session()     # No application context.
