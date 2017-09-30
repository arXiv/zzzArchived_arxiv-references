"""Initialize the Celery application."""

from celery import Celery
from flask import Flask
import os
from references import celeryconfig
flask_app = Flask('references')
flask_app.config.from_pyfile('config.py')


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""

    def __init__(self, *args, **kwargs):
        super(MetaCelery, self).__init__(*args, **kwargs)
        self.config = {}
        

REDIS_ENDPOINT = os.environ.get('REDIS_ENDPOINT')
broker_url = "redis://%s/0" % REDIS_ENDPOINT
result_backend = "redis://%s/0" % REDIS_ENDPOINT
app = MetaCelery('references', results=result_backend, broker=broker_url)
app.config_from_object(celeryconfig)
app.config.update(flask_app.config)
app.autodiscover_tasks(['references.process', 'references.agent'])
app.conf.task_default_queue = 'references-worker'
