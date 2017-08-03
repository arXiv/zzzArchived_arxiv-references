"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""

import os
from urllib import parse

# AWS_ACCESS_KEY = os.environ.get('REFLINK_AWS_ACCESS_KEY')
# AWS_SECRET_KEY = os.environ.get('REFLINK_AWS_SECRET_KEY', '')
# AWS_SECRET_KEY = parse.quote(AWS_SECRET_KEY, safe='')
# broker_url = "sqs://{}:{}@".format(AWS_ACCESS_KEY, AWS_SECRET_KEY)
REFLINK_REDIS_ENDPOINT = os.environ.get('REFLINK_REDIS_ENDPOINT')
broker_url = "redis://%s" % REFLINK_REDIS_ENDPOINT
backend = "redis://%s" % REFLINK_REDIS_ENDPOINT
broker_transport_options = {
    'region': os.environ.get('AWS_REGION', 'us-east-1'),
    'queue_name_prefix': 'reflink-',
}
worker_prefetch_multiplier = 0
