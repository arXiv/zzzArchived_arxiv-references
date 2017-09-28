from flask import Flask
import os
import logging


def create_cermine_app() -> Flask:
    """Initialize an instance of the CERMINE extractor."""
    from extract import routes
    app = Flask('cermine')
    app.config['UPLOAD_PATH'] = os.environ.get('UPLOAD_PATH', '/tmp/uploads')
    app.config['LOGFILE'] = os.environ.get('LOGFILE', None)
    loglevel = os.environ.get('LOGLEVEL', logging.INFO)
    if isinstance(loglevel, str):
        try:
            loglevel = int(loglevel)
        except ValueError:
            loglevel = logging.INFO
    app.config['LOGLEVEL'] = loglevel
    app.register_blueprint(routes.blueprint)
    return app
