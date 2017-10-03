"""Retrieves PDFs of published papers from the core arXiv document store."""

from references.services.retrieve import retrievePDF
from references.services.metrics import metrics


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
    return retrievePDF.session.is_valid_url(url)


@metrics.session.reporter
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
    metrics_data = []
    try:
        pdf_path = retrievePDF.session.retrieve(url, document_id)
    except IOError as e:
        metrics_data.append({'metric': 'PDFIsAvailable', 'value': 0.})
        pdf_path = None
    except ValueError as e:
        raise ValueError('%s: %s' % (document_id, e)) from e
    if pdf_path is None:
        metrics_data.append({'metric': 'PDFIsAvailable', 'value': 0.})
    else:
        metrics_data.append({'metric': 'PDFIsAvailable', 'value': 1.})
    return pdf_path, metrics_data
