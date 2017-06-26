import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


from reflink.types import ReferenceMetadata
from reflink.services import data_store, object_store
from celery import shared_task


@shared_task
def store_metadata(metadata: ReferenceMetadata,
                   document_id: str) -> ReferenceMetadata:
    """
    Deposit extracted reference metadata in the datastore.

    Parameters
    ----------
    document_id : str
    metadata : list

    Returns
    -------
    metadata : list

    Raises
    ------
    RuntimeError
        Raised when there is a problem storing the metadata. The caller should
        assume that nothing has been stored.
    """
    try:
        data_store.get_session().create(document_id, metadata)
    except IOError as e:    # Separating this out in case we want to retry.
        msg = 'Could not store metadata for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    except Exception as e:
        msg = 'Could not store metadata for document %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg) from e
    return metadata


@shared_task
def store_pdf(pdf_path: str, document_id: str) -> None:
    """
    Deposit link-injected PDF in the object store.

    Parameters
    ----------
    pdf_path : str
    document_id : str

    Raises
    ------
    RuntimeError
        Raised when there is a problem storing the PDF. The caller should assume
        that nothing has been stored.
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
