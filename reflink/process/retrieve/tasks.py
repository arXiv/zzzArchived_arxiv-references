"""Registry for retrieve tasks. Celery will look here for tasks to load."""

from .fake import fake_retrieve as retrieve
from celery import Task

assert isinstance(retrieve, Task)
