"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""
import os
from urllib import parse


AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
# AWS_SECRET_KEY = parse.quote(AWS_SECRET_KEY, safe='')
# broker_url = "sqs://{}:{}@".format(AWS_ACCESS_KEY, AWS_SECRET_KEY)
REDIS_HOST = os.environ.get('REDIS_MASTER_SERVICE_HOST')
REDIS_PORT = os.environ.get('REDIS_MASTER_SERVICE_PORT')
REDIS_ENDPOINT = os.environ.get('REDIS_ENDPOINT', '%s:%s' % (REDIS_HOST, REDIS_PORT))
broker_url = "redis://%s/0" % REDIS_ENDPOINT
result_backend = "redis://%s/0" % REDIS_ENDPOINT
broker_transport_options = {
    'region': os.environ.get('AWS_REGION', 'us-east-1'),
    'queue_name_prefix': 'references-',
}
worker_prefetch_multiplier = 1
task_acks_late = True
