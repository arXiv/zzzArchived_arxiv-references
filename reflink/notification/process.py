"""Provides an interface to Celery for the notification consumer."""

from reflink.process.orchestrate import tasks
from celery.exceptions import TaskError


def process_document(document_id: str) -> None:
    """
    Generate an asynchronous task to process a single document.

    Parameters
    ----------
    document_id : str

    Raises
    ------
    RuntimeError
    """
    try:
        tasks.process_document.delay(document_id)
    except TaskError as e:
        raise RuntimeError(str(e)) from e
