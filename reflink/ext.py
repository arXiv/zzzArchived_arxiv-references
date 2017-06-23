"""
Adapted from https://github.com/inveniosoftware/invenio-app/blob/master/invenio_app/ext.py
"""

import pkg_resources
from flask_limiter import Limiter
from flask_limiter.util import get_ipaddr
from flask_talisman import Talisman

from . import config


class ReflinkApp(object):
    def __init__(self, app=None, **kwargs):
        self.limiter = None
        self.talisman = None

        if app:
            self.init_app(app, **kwargs)

    def init_app(self, app, **kwargs):
        self.init_config(app)    # Initialize the configuration.
        self.limiter = Limiter(app, key_func=get_ipaddr)  # Enable Rate limiter.
        # Enable secure HTTP headers
        if app.config['APP_ENABLE_SECURE_HEADERS']:
            self.talisman = Talisman(
                app, **app.config.get('APP_DEFAULT_SECURE_HEADERS', {})
            )
        app.extensions['reflink'] = self    # Register self

    def init_config(self, app):
        """
        Initialize configuration.
        """
        config_apps = ['APP_', 'RATELIMIT_']
        for k in dir(config):
            if any([k.startswith(prefix) for prefix in config_apps]):
                app.config.setdefault(k, getattr(config, k))
