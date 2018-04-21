"""Provides an application factory for CERMINE extractor."""

from flask import Flask
import os
import logging
import tempfile


def create_cermine_app() -> Flask:
    """Initialize an instance of the CERMINE extractor."""
    from extract import routes
    app = Flask('cermine')
    app.config['UPLOAD_PATH'] = os.environ.get('UPLOAD_PATH',
                                               tempfile.mkdtemp())
    app.config['LOGFILE'] = os.environ.get('LOGFILE', None)
    app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False
    loglevel = os.environ.get('LOGLEVEL', logging.INFO)
    if isinstance(loglevel, str):
        try:
            loglevel = int(loglevel)
        except ValueError:
            loglevel = logging.INFO
    app.config['LOGLEVEL'] = loglevel
    app.register_blueprint(routes.blueprint)
    return app
