"""Web Server Gateway Interface entry-point."""

from references.factory import create_extractor_app
import os


def application(environ, start_response):
    for key, value in environ.items():
        os.environ[key] = str(value)
    app = create_extractor_app()
    return app(environ, start_response)
