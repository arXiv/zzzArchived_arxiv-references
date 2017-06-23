import sys, os

from reflink import config
from reflink.views import rest

from flask import Flask

app_class = Flask

def create_app():
    app = Flask('reflink')
    app.config.from_pyfile('config.py')
    app.register_blueprint(rest.blueprint)
    return app
