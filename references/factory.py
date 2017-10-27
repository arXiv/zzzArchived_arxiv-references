"""Application factory for references service components."""

from flask import Flask
from celery import Celery
from references import celeryconfig
from references.services import credentials, data_store, cermine, grobid
from references.services import refextract, retrieve

from references import routes
import logging


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""

    def __init__(self, *args, **kwargs):
        super(MetaCelery, self).__init__(*args, **kwargs)
        self.config = {}


def create_web_app() -> Flask:
    """Initialize an instance of the extractor backend service."""
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    app = Flask('references', static_folder='static',
                template_folder='templates')
    app.config.from_pyfile('config.py')

    if app.config.get('INSTANCE_CREDENTIALS') == 'true':
        credentials.init_app(app)
        credentials.current_session(app)   # Will get fresh creds.

    data_store.init_app(app)
    cermine.init_app(app)
    grobid.init_app(app)
    refextract.init_app(app)
    retrieve.init_app(retrieve)
    app.register_blueprint(routes.blueprint)

    celery = Celery(app.name, results=celeryconfig.result_backend,
                    broker=celeryconfig.broker_url)
    celery.config_from_object(celeryconfig)
    celery.autodiscover_tasks(['references.process'])
    celery.conf.task_default_queue = 'references-worker'
    return app


def create_worker_app() -> Celery:
    """Initialize an instance of the worker application."""

    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    flask_app = Flask('references')
    flask_app.config.from_pyfile('config.py')

    app = MetaCelery(flask_app.name, results=celeryconfig.result_backend,
                     broker=celeryconfig.broker_url)
    app.config_from_object(celeryconfig)
    app.config.update(flask_app.config)

    if app.config.get('INSTANCE_CREDENTIALS') == 'true':
        credentials.init_app(app)
        credentials.current_session(app)   # Will get fresh creds.

    app.autodiscover_tasks(['references.process'], force=True)
    app.conf.task_default_queue = 'references-worker'
    data_store.init_app(app)
    cermine.init_app(app)
    grobid.init_app(app)
    refextract.init_app(app)
    retrieve.init_app(retrieve)
    return app
