"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""

import os

broker_url = 'redis:///'
task_always_eager = os.environ.get('CELERY_TASK_ALWAYS_EAGER') == 'yes'
