"""Encapsulates storage logic for the processing pipeline."""

from .tasks import store_metadata, store_pdf
from celery import Task

assert isinstance(store_metadata, Task)
assert isinstance(store_pdf, Task)
