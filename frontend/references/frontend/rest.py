"""Provides REST API routes."""

from flask.json import jsonify
from flask import Blueprint, render_template, redirect, request

from references.web.references import ReferenceMetadataController
from references.web.health import health_check
from references.types import ViewResponseData
from references import status

blueprint = Blueprint('references_api', __name__, url_prefix='')


# TODO: remove this before production deploy.
@blueprint.route('/abs/<string:document_id>', methods=['GET'])
def abs(document_id: str) -> ViewResponseData:
    """Demo route for abs page."""
    return render_template('abs.html', document_id=document_id)


@blueprint.route('/status', methods=['GET'])
def ok() -> ViewResponseData:
    """Provide current integration status information for health checks."""
    return jsonify(health_check()), status.HTTP_200_OK


@blueprint.route('/references/<string:doc_id>/ref/<string:ref_id>/resolve',
                 methods=['GET'])
def resolve_reference(doc_id: str, ref_id: str) -> ViewResponseData:
    """
    Forward a user to a resource for a specific reference.

    Parameters
    ----------
    doc_id : str
    ref_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`references.status` for details.
    """
    controller = ReferenceMetadataController()
    url, status_code = controller.resolve(doc_id, ref_id)
    if status_code != status.HTTP_303_SEE_OTHER:
        return jsonify(url), status_code
    return redirect(url, code=status_code)


@blueprint.route('/references/<string:doc_id>/ref/<string:ref_id>',
                 methods=['GET'])
def get_reference_metadata(doc_id: str, ref_id: str) -> ViewResponseData:
    """
    Retrieve metadata for a specific reference in an arXiv publication.

    Parameters
    ----------
    doc_id : str
    ref_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`references.status` for details.
    """
    controller = ReferenceMetadataController()
    response, status_code = controller.get(doc_id, ref_id)
    return jsonify(response), status_code


@blueprint.route('/references/<string:doc_id>', methods=['GET'])
def get_reference_metadata_list(doc_id: str) -> ViewResponseData:
    """
    Retrieve all reference metadata for an arXiv publication.

    Parameters
    ----------
    doc_id : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`references.status` for details.
    """
    controller = ReferenceMetadataController()
    reftype = request.args.get('reftype', '__all__')
    response, status_code = controller.list(doc_id, reftype=reftype)
    return jsonify(response), status_code
