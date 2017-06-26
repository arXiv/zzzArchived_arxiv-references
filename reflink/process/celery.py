from celery import Celery
from . import config


app = Celery()
app.config_from_object(config)
app.autodiscover_tasks(['store', 'retrieve', 'extract', 'inject'])
