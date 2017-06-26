"""Fake implementation of the inject task."""

from celery import shared_task

import tempfile


@shared_task
def fake_inject(source_path: str) -> str:
    """
    Emulate injecting links into a PDF using reference metadata.

    Parameters
    ----------
    source_path : str
        Location of TeX source bundle on filesystem.

    Returns
    -------
    pdf_path : str
        Location of rendered PDF on the filesystem.
    """
    _, pdf_path = tempfile.mkstemp()
    return pdf_path
