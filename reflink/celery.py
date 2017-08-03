"""Initialize the Celery application."""

from celery import Celery
from flask import Flask
import os
from reflink import celeryconfig
flask_app = Flask('reflink')
flask_app.config.from_pyfile('config.py')


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""

    def __init__(self, *args, **kwargs):
        super(MetaCelery, self).__init__(*args, **kwargs)
        self.config = {}
        

REFLINK_REDIS_ENDPOINT = os.environ.get('REFLINK_REDIS_ENDPOINT')
broker_url = "redis://%s/0" % REFLINK_REDIS_ENDPOINT
result_backend = "redis://%s/0" % REFLINK_REDIS_ENDPOINT
app = MetaCelery('reflink', results=result_backend, broker=broker_url)
app.config_from_object(celeryconfig)
app.config.update(flask_app.config)
app.autodiscover_tasks(['reflink.process', 'reflink.agent'])
app.conf.task_default_queue = 'reflink-worker'
