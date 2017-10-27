"""Retrieves PDFs of published papers from the core arXiv document store."""


from references.services import metrics
from references.services import retrieve as retrieve_service


def is_valid_url(url: str) -> bool:
    """
    Evaluate whether or not a URL is acceptible for retrieval.

    Parameters
    ----------
    url : str
        Location of a document.

    Returns
    -------
    bool
    """
    return retrieve_service.is_valid_url(url)


def retrieve(url: str, document_id: str) -> tuple:
    """
    Retrieve PDF for an arXiv document.

    Parameters
    ----------
    url : str
    document_id : str

    Returns
    -------
    pdf_path : str
    """
    try:
        pdf_path = retrieve_service.retrieve_pdf(url, document_id)
    except IOError as e:
        metrics.report('PDFIsAvailable', 0.)
        pdf_path = None
    except ValueError as e:
        raise ValueError('%s: %s' % (document_id, e)) from e
    if pdf_path is None:
        metrics.report('PDFIsAvailable', 0.)
    else:
        metrics.report('PDFIsAvailable', 1.)
    return pdf_path
