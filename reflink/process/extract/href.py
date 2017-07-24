"""
HREF extraction module.

Provides a wrapper for `PDFx <https://www.metachris.com/pdfx/>`_, which
extracts links from PDFs.
"""

import pdfx
import logging
logging.getLogger().setLevel(logging.ERROR)


def _transform(metadatum: pdfx.backends.Reference) -> dict:
    """
    Transform pdfx reference metadata into a valid extraction.

    Parameters
    ----------
    :class:`pdfx.backends.Reference`

    Returns
    -------
    dict
    """
    href = metadatum.ref
    if not href.startswith('http'):
        href = 'http://%s' % href

    return {
        'reftype': 'href',
        'href': href,
        'raw': metadatum.ref,
    }


def extract_references(pdf_path):
    """Extract HREFs from an arXiv PDF."""

    pdf = pdfx.PDFx(pdf_path)
    return list(map(_transform, list(pdf.get_references())))
