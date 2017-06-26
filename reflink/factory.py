"""
Application factory for reflink service components.
"""

from flask import Flask


def create_web_app():
    """
    Initialize an instance of the web application.
    """
    from reflink.web.views import rest
    app = Flask('reflink')
    app.config.from_pyfile('config.py')
    app.register_blueprint(rest.blueprint)
    return app


def create_process_app():
    """
    Initialize an instance of the processing application.
    """
    from reflink.process.celery import app
    return app
