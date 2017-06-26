"""Initialize the Celery application."""

from celery import Celery
from . import config


app = Celery()
app.config_from_object(config)
app.autodiscover_tasks(['reflink.process.store', 'reflink.process.retrieve',
                        'reflink.process.extract', 'reflink.process.inject'])
