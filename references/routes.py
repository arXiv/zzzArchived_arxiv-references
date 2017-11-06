"""Provides REST API routes."""

from flask.json import jsonify
from flask import Blueprint, render_template, redirect, request, url_for

from references.controllers import extracted_references, extraction
from references.controllers.health import health_check
from references import status


blueprint = Blueprint('references', __name__, url_prefix='')


@blueprint.route('/status', methods=['GET'])
def ok() -> tuple:
    """Provide current integration status information for health checks."""
    return jsonify(health_check()), status.HTTP_200_OK


@blueprint.route('/references', methods=['POST'])
def extract_references() -> tuple:
    """Handle requests for reference extraction."""
    data, code, headers = extraction.extract(request.get_json(force=True))
    return jsonify(data), code, headers


@blueprint.route('/status/<string:task_id>', methods=['GET'])
def task_status(task_id: str) -> tuple:
    """Get the status of a reference extraction task."""
    data, code, headers = extraction.status(task_id)
    return jsonify(data), code, headers


@blueprint.route('/references/<arxiv:doc_id>/ref/<string:ref_id>/resolve',
                 methods=['GET'])
def resolve_reference(doc_id: str, ref_id: str) -> tuple:
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
    content, status_code, _ = extracted_references.resolve(doc_id, ref_id)
    if status_code != status.HTTP_303_SEE_OTHER:
        return jsonify(content), status_code
    return redirect(content.get('url'), code=status_code)


@blueprint.route('/references/<arxiv:doc_id>/ref/<string:ref_id>',
                 methods=['GET'])
def reference(doc_id: str, ref_id: str) -> tuple:
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
    response, status_code = extracted_references.get(doc_id, ref_id)
    return jsonify(response), status_code


@blueprint.route('/references/<arxiv:doc_id>', methods=['GET'])
def references(doc_id: str) -> tuple:
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
    reftype = request.args.get('reftype', '__all__')
    response, status_code = extracted_references.list(doc_id, reftype=reftype)
    return jsonify(response), status_code


@blueprint.route('/references/<arxiv:doc_id>/raw/<string:extractor>',
                 methods=['GET'])
def raw(doc_id: str, extractor: str) -> tuple:
    """
    Retrieve raw reference metadata for a specific extractor.

    Parameters
    ----------
    doc_id : str
    extractor : str

    Returns
    -------
    :class:`flask.Response`
        JSON response.
    int
        HTTP status code. See :mod:`references.status` for details.
    """
    response, status_code = extracted_references.get_raw_extraction(doc_id,
                                                                    extractor)
    return jsonify(response), status_code
