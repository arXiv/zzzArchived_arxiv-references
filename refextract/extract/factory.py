from flask import Flask, request
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

    @app.before_request
    def before_request():
        print(request.__dict__)
        print("The route is: {}".format(request.url_rule))
    return app
