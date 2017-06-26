"""A fake implementation of the extraction task."""

from celery import shared_task
from reflink.types import ReferenceMetadata


@shared_task
def fake_extract(pdf_path: str) -> ReferenceMetadata:
    """
    Emulate extracting references from a PDF.

    Parameters
    ----------
    pdf_path : str
        Location of an arXiv PDF on filesystem.

    Returns
    -------
    reference_metadata : list
        A list of (dict) reference metadata. Should contain the original
        reference source line from which the metadata was parsed.
    """
    reference_metadata = [
        {'the': 'first', 'original': 'The first one'},
        {'the': 'second', 'original': 'The second one'}
    ]
    return reference_metadata
