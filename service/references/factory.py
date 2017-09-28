"""Application factory for references service components."""

from flask import Flask
from celery import Celery

import logging


def create_web_app() -> Flask:
    """Initialize an instance of the extractor backend service."""
    from references import routes
    from references.services.data_store import referencesStore
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    app = Flask('references', static_folder='static',
                template_folder='templates')
    app.config.from_pyfile('config.py')
    referencesStore.init_app(app)
    app.register_blueprint(routes.blueprint)
    return app


def create_worker_app() -> Celery:
    """Initialize an instance of the worker application."""
    from references.celery import app
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    return app
