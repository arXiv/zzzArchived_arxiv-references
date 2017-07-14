"""Web Server Gateway Interface entry-point."""

from reflink.factory import create_web_app
try:
    from reflink import secrets
except ImportError:
    pass

application = create_web_app()
