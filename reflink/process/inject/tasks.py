"""Registry for injection tasks. Celery will look here for tasks to load."""

from .fake import fake_inject as inject
from celery import Task

assert isinstance(inject, Task)
