from flask import Flask
import os
import logging


def create_refextract_app() -> Flask:
    """Initialize an instance of the extractor backend service."""
    from extract import routes
    app = Flask('refextract')
    app.config['UPLOAD_PATH'] = os.environ.get('UPLOAD_PATH', '/tmp/uploads')
    app.config['LOGFILE'] = os.environ.get('LOGFILE', None)
    app.config['LOGLEVEL'] = os.environ.get('LOGLEVEL', logging.INFO)
    app.register_blueprint(routes.blueprint)
    return app
