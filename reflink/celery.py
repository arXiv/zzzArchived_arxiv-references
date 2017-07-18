"""Initialize the Celery application."""

from celery import Celery
# from flask import Flask

from reflink import celeryconfig
# flask_app = Flask('reflink')
# flask_app.config.from_pyfile('config.py')


app = Celery('reflink')
app.config_from_object(celeryconfig)
app.autodiscover_tasks(['reflink.process.orchestrate'])
app.conf.task_default_queue = 'reflink-worker'
print(app.conf.__dict__)
