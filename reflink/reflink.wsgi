"""Web Server Gateway Interface entry-point."""
import sys
sys.path.append('.')
from reflink.factory import create_web_app

application = create_web_app()
