"""Registry for extraction tasks. Celery will look here for tasks to load."""

from celery import shared_task

from reflink.process.extract import cermine

@shared_task
def extract_cermine(paths: tuple) -> tuple:
    """
    A fake implementation of the extraction task.

    Emulate extracting references from a PDF.

    Parameters
    ----------
    paths : tuple
        Contains the location of an arXiv PDF and TeX source bundle on the
        filesystem, in that order.

    Returns
    -------
    tuple
        Contains the location of an arXiv PDF and TeX source bundle on the
        filesystem, and the extracted reference metadata (list), in that order.
    """
    pdf_path, source_path = paths
    reference_metadata = cermine.extract_references(pdf_path)
    return pdf_path, source_path, reference_metadata


extract = extract_cermine
