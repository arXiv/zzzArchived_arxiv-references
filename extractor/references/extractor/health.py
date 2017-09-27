"""Provides health-check controller(s)."""

from references.services.cermine import cermine
from references.services.data_store import referencesStore
from references.services.events import extractionEvents
from references.services.grobid import grobid
from references.services.metrics import metrics
from references.services.refextract import refExtract


def _healthy_session(service):
    """Evaluate whether we have an healthy session with ``service``."""
    try:
        print(service.session)
    except Exception as e:
        return False
    return True


def health_check() -> dict:
    """Retrieve the current health of service integrations."""
    return {
        'datastore': _healthy_session(referencesStore),
        'events': _healthy_session(extractionEvents),
        'metrics': _healthy_session(metrics),
        'grobid': _healthy_session(grobid),
        'cermine': _healthy_session(cermine),
        'refextract': _healthy_session(refExtract),
    }
