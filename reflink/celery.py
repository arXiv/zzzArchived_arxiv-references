"""Initialize the Celery application."""

from celery import Celery
from . import celeryconfig


app = Celery()
app.config_from_object(celeryconfig)
app.autodiscover_tasks(['reflink.process.store', 'reflink.process.retrieve',
                        'reflink.process.extract', 'reflink.process.inject'])
