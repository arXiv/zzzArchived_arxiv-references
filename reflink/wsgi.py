"""
Web Server Gateway Interface entry-point.
"""

from .factory import create_web_app

application = create_web_app()
