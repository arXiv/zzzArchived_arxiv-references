"""Registry for extraction tasks. Celery will look here for tasks to load."""

from .fake import fake_extract as extract

from celery import Task

assert isinstance(extract, Task)
