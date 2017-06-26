"""Registry for injection tasks. Celery will look here for tasks to load."""

from celery import shared_task

import tempfile


@shared_task
def fake_inject(paths_and_metadata: tuple) -> str:
    """
    Fake implementation of the inject task.

    Emulate injecting links into a PDF using reference metadata.

    Parameters
    ----------
    paths_and_metadata : tuple
        Contains the location of an arXiv PDF and TeX source bundle on the
        filesystem, and the reference metadata, in that order.

    Returns
    -------
    tuple
        Reference metadata (list) and path to link-injected PDF.
    """
    pdf_path, source_path, reference_metadata = paths_and_metadata
    _, pdf_path = tempfile.mkstemp()
    return reference_metadata, pdf_path


inject = fake_inject
