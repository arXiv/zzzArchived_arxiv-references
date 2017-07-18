"""Provides REST API routes."""

from flask.json import jsonify
from flask import Blueprint, render_template, redirect

from reflink.web.controllers import references, pdf
from reflink.types import ViewResponseData
from reflink import status

blueprint = Blueprint('reflink_api', __name__, url_prefix='')


@blueprint.route('/abs/<string:document_id>', methods=['GET'])
def abs(document_id: str) -> ViewResponseData:
    return render_template('abs.html', document_id=document_id)


@blueprint.route('/status', methods=['GET'])
def ok() -> ViewResponseData:
    return jsonify({'hello': 'world'}), status.HTTP_200_OK


@blueprint.route('/references/<string:document_id>/ref/<string:reference_id>/resolve',
                 methods=['GET'])
def resolve_reference(document_id: str, reference_id: str) -> ViewResponseData:
    """
    Forward a user to a resource for a specific reference.

    Parameters
    ----------
    document_id : str
    reference_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`reflink.status` for details.
    """
    controller = references.ReferenceMetadataController()
    url, status_code = controller.resolve(document_id, reference_id)
    if status_code != status.HTTP_303_SEE_OTHER:
        return jsonify(url), status_code
    return redirect(url, code=status_code)


@blueprint.route('/references/<string:document_id>/ref/<string:reference_id>',
                 methods=['GET'])
def get_reference_metadata(document_id: str, reference_id: str) -> ViewResponseData:
    """
    Retrieve metadata for a specific reference in an arXiv publication.

    Parameters
    ----------
    document_id : str
    reference_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`reflink.status` for details.
    """
    controller = references.ReferenceMetadataController()
    response, status_code = controller.get(document_id, reference_id)
    return jsonify(response), status_code


@blueprint.route('/references/<string:document_id>', methods=['GET'])
def get_reference_metadata_list(document_id: str) -> ViewResponseData:
    """
    Retrieve all reference metadata for an arXiv publication.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`reflink.status` for details.
    """
    controller = references.ReferenceMetadataController()
    response, status_code = controller.list(document_id)
    return jsonify(response), status_code


@blueprint.route('/pdf/<string:document_id>', methods=['GET'])
def get_pdf_location(document_id: str) -> ViewResponseData:
    """
    Retrieve the location of a link-injected PDF for an arXiv publication.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`reflink.status` for details.
    """
    response, status_code = pdf.PDFController().get(document_id)
    return jsonify(response), status_code
