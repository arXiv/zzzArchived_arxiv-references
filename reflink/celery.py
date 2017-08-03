"""Initialize the Celery application."""

from celery import Celery
# from flask import Flask

from reflink import celeryconfig
# flask_app = Flask('reflink')
# flask_app.config.from_pyfile('config.py')


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""
    @property
    def config(self):
        return self.conf


app = MetaCelery('reflink')
app.config_from_object(celeryconfig)
app.autodiscover_tasks(['reflink.process'])
app.conf.task_default_queue = 'reflink-worker'
