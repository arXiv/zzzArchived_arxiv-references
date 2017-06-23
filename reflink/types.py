"""
Defines custom types for type hints.
"""

from typing import Tuple, TypeVar
import flask

ControllerResponseData = Tuple[dict, int]
ViewResponseData = Tuple[flask.Response, int]
IntOrNone = TypeVar('IntOrNone', int, None)
BytesOrNone = TypeVar('BytesOrNone', bytes, None)
