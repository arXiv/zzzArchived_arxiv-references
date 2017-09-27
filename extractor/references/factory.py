"""Application factory for references service components."""

from flask import Flask
from celery import Celery

import logging


def create_frontend_web_app() -> Flask:
    """Initialize an instance of the frontend service."""
    from references.web import frontend
    from references.services.data_store import referencesStore
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    app = Flask('references', static_folder='web/static',
                template_folder='web/templates')
    app.config.from_pyfile('config.py')
    referencesStore.init_app(app)
    app.register_blueprint(frontend.blueprint)
    return app


def create_extractor_app() -> Flask:
    """Initialize an instance of the extractor backend service."""
    from references.web import extractor
    from references.services.data_store import referencesStore
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    app = Flask('references', static_folder='web/static',
                template_folder='web/templates')
    app.config.from_pyfile('config.py')
    referencesStore.init_app(app)
    app.register_blueprint(extractor.blueprint)
    return app
    


def create_process_app() -> Celery:
    """Initialize an instance of the processing application."""
    from references.celery import app
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    return app
