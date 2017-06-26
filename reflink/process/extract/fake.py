from celery import shared_task

from reflink.types import ReferenceMetadata

import tempfile


@shared_task
def fake_extract(pdf_path: str) -> ReferenceMetadata:
    """
    Emulates extracting references from a PDF.

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
