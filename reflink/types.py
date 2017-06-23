"""
Defines custom types for type hints.
"""

from typing import Tuple
import flask

ControllerResponseData = Tuple[dict, int]
ViewResponseData = Tuple[flask.Response, int]
