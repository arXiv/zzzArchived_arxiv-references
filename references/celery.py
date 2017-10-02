"""Initialize the Celery application."""

from references.factory import create_worker_app

app = create_worker_app()
