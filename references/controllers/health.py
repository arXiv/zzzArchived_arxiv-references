"""Provides health-check controller(s)."""

from typing import Tuple, Any

from references.services import cermine, data_store, grobid
from references.services import refextract

from arxiv.base import logging
logger = logging.getLogger(__name__)

ControllerResponse = Tuple[dict, int, dict]


def _getServices() -> list:
    """Yield a list of services to check for connectivity."""
    return [
        ('datastore', data_store),
        ('grobid', grobid),
        ('cermine', cermine),
        ('refextract', refextract),
    ]


def _healthy_session(service: Any) -> bool:
    """Evaluate whether we have an healthy session with ``service``."""
    try:
        if hasattr(service, 'session'):
            service.session
        else:
            service.current_session()
    except Exception as e:
        logger.info('Could not initiate session for %s: %s' %
                    (str(service), e))
        return False
    return True


def health_check() -> ControllerResponse:
    """
    Retrieve the current health of service integrations.

    Returns
    -------
    dict
        Response content.
    int
        HTTP status code.
    dict
        Response headers.
    """
    status = {}
    for name, obj in _getServices():
        logger.info('Getting status of %s' % name)
        status[name] = _healthy_session(obj)
    return status, 200, {}
