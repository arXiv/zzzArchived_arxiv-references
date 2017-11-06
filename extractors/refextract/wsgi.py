"""Web Server Gateway Interface entry-point."""

from extract.factory import create_refextract_app
import os


def application(environ, start_response):
    """WSGI application factory."""
    for key, value in environ.items():
        os.environ[key] = str(value)
    app = create_refextract_app()
    return app(environ, start_response)
