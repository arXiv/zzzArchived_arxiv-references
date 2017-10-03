"""Application factory for references service components."""

from flask import Flask
from celery import Celery
from references import celeryconfig

import logging


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""

    def __init__(self, *args, **kwargs):
        super(MetaCelery, self).__init__(*args, **kwargs)
        self.config = {}


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
    referencesStore.session.create_table()
    referencesStore.session.extractions.create_table()
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
    app.autodiscover_tasks(['references.process'], force=True)
    app.conf.task_default_queue = 'references-worker'
    print(app)
    return app
