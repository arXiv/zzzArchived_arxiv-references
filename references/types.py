"""Custom types for type hints."""

try:
    from typing import Tuple, TypeVar, List, Generator, Callable
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
    Callable = Generic()

import flask


ControllerResponseData = Tuple[dict, int]
"""
Represents response data for rendering in a view, and an accompanying HTTP
status code.
"""


IntOrNone = TypeVar('IntOrNone', int, None)
BytesOrNone = TypeVar('BytesOrNone', bytes, None)

ReferenceMetadata = List[dict]
