"""Custom types for type hints."""

try:
    from typing import Tuple, TypeVar, List, Generator
except ImportError:
    class Generic(object):
        """Mocks the typing generic types for Python versions < 3.5."""
        
        def __getitem__(self, *args):
            return Generic()

        def __call__(self, *args):
            return Generic()

    Tuple = Generic()
    TypeVar = Generic()
    List = Generic()
    Generator = Generic()

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

ReferenceMetadata = List[dict]
