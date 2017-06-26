from celery import Celery

app = Celery()
app.config_from_object('reflink.celeryconfig')
app.autodiscover_tasks(['reflink.tasks.store',
                        'reflink.tasks.retrieve',
                        'reflink.tasks.extract',
                        'reflink.tasks.inject'])
