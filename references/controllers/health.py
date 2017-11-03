"""Provides health-check controller(s)."""

from typing import Tuple

from references.services import cermine, data_store, metrics, grobid
from references.services import refextract

from references import logging
logger = logging.getLogger(__name__)

ControllerResponse = Tuple[dict, int, dict]


def _getServices() -> list:
    """Yield a list of services to check for connectivity."""
    return [
        ('datastore', data_store),
        ('metrics', metrics),
        ('grobid', grobid),
        ('cermine', cermine),
        ('refextract', refextract),
    ]


def _healthy_session(service):
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
    return {}, status, {}
