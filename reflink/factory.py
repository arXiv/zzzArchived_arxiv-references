import sys, os

from .web import config

from flask import Flask

app_class = Flask


def create_web_app():
    app = Flask('reflink')
    app.config.from_pyfile('config.py')
    app.register_blueprint(rest.blueprint)
    return app


def create_process_app():
    from reflink.process.celery import app
    return app
