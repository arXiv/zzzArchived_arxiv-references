"""Web Server Gateway Interface entry-point."""

from reflink.factory import create_web_app

application = create_web_app()
