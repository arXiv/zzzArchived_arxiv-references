"""Application factory for references service components."""

import logging

from flask import Flask
from celery import Celery
from references import celeryconfig

from references.services import data_store, cermine, grobid, refextract, \
    retrieve
from references import routes

celery_app = Celery(__name__, results=celeryconfig.result_backend,
                    broker=celeryconfig.broker_url)
celery_app.config_from_object(celeryconfig)
celery_app.autodiscover_tasks(['references.process'], force=True)
celery_app.conf.task_default_queue = 'references-worker'


def create_web_app() -> Flask:
    """Initialize an instance of the extractor backend service."""
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    app = Flask('references', static_folder='static',
                template_folder='templates')
    app.config.from_pyfile('config.py')
    from references.converter import ArXivConverter
    app.url_map.converters['arxiv'] = ArXivConverter

    data_store.init_app(app)
    cermine.init_app(app)
    grobid.init_app(app)
    refextract.init_app(app)
    retrieve.init_app(retrieve)
    app.register_blueprint(routes.blueprint)
    return app


def create_worker_app() -> Celery:
    """Initialize an instance of the worker application."""
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    flask_app = Flask('references')
    flask_app.config.from_pyfile('config.py')
    celery_app.conf.update(flask_app.config)
    data_store.init_app(flask_app)
    cermine.init_app(flask_app)
    grobid.init_app(flask_app)
    refextract.init_app(flask_app)
    retrieve.init_app(flask_app)
    return flask_app
