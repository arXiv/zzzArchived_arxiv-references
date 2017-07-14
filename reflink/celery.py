"""Initialize the Celery application."""

from celery import Celery
from flask import Flask

from reflink import celeryconfig
flask_app = Flask('reflink')
flask_app.config.from_pyfile('config.py')


app = Celery(flask_app.name)
app.config_from_object(celeryconfig)
app.autodiscover_tasks(['reflink.process.store', 'reflink.process.retrieve',
                        'reflink.process.extract', 'reflink.process.inject'])
