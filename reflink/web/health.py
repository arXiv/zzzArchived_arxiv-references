"""Provides health-check controller(s)."""

from reflink.services.cermine import cermine
from reflink.services.data_store import referencesStore
from reflink.services.events import extractionEvents
from reflink.services.grobid import grobid
from reflink.services.metrics import metrics
from reflink.services.refextract import refExtract


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
