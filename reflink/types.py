"""
Custom types for type hints.
"""

from typing import Tuple, TypeVar, List
import flask


ControllerResponseData = Tuple[dict, int]
"""
Represents response data for rendering in a view, and an accompanying HTTP
status code.
"""

ViewResponseData = Tuple[flask.Response, int]
"""
A Flask response and accompanying HTTP status code.
"""

IntOrNone = TypeVar('IntOrNone', int, None)
BytesOrNone = TypeVar('BytesOrNone', bytes, None)

PathTuple = Tuple[str, str]

ReferenceMetadata = List[dict]
