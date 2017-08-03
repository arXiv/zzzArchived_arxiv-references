"""Encapsulates storage logic for the processing pipeline."""


import logging

from reflink.services import object_store
from reflink.types import ReferenceMetadata

log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(__name__)


def store_pdf(pdf_path: str, document_id: str) -> None:
    """
    Deposit link-injected PDF in the object store.

    Parameters
    ----------
    pdf_path : str
        The location of the link-injected PDF on the filesystem.
    document_id : str

    Raises
    ------
    RuntimeError
        Raised when there is a problem storing the PDF. The caller should
        assumethat nothing has been stored.
    """
    try:
        object_store.get_session().create(document_id, pdf_path)
    except IOError as e:    # Separating this out in case we want to retry.
        msg = 'Could not store PDF for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    except Exception as e:
        msg = 'Could not store PDF for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
