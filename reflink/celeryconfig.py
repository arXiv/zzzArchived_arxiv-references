"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""

import os
from urllib import parse

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = parse.quote(os.environ.get('AWS_SECRET_KEY', ''), safe='')
broker_url = "sqs://{}:{}@".format(AWS_ACCESS_KEY, AWS_SECRET_KEY)
broker_transport_options = {
    'region': os.environ.get('AWS_REGION', 'us-east-1'),
    'queue_name_prefix': 'reflink-',
}
worker_prefetch_multiplier = 0
