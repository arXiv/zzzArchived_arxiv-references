"""Application factory for reflink service components."""

from flask import Flask
import logging


def create_web_app():
    """Initialize an instance of the web application."""
    from reflink.web.routes import rest
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    app = Flask('reflink')
    app.config.from_pyfile('config.py')
    app.register_blueprint(rest.blueprint)
    return app


def create_process_app():
    """Initialize an instance of the processing application."""
    from reflink.celery import app
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    return app
