"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""

import os

broker_url = os.environ.get('REFLINK_SQS_ENDPOINT', 'redis:///')
broker_transport_options = {
    'region': os.environ.get('AWS_REGION', 'us-east-1'),
}
result_backend = 'redis:///'
task_always_eager = os.environ.get('CELERY_TASK_ALWAYS_EAGER') == 'yes'
