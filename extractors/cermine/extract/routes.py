"""HTTP routes for CERMINE extraction API."""

import os
import tempfile
import shutil

from flask.json import jsonify
from flask import Blueprint, request, current_app, Response
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage

from arxiv.base import logging

from .extract import extract_with_cermine

logger = logging.getLogger(__name__)

HTTP_200_OK = 200
HTTP_400_BAD_REQUEST = 400
HTTP_500_INTERNAL_SERVER_ERROR = 500


blueprint = Blueprint('cermine', __name__, url_prefix='/cermine')


def xmlify(response_data: dict) -> Response:
    """Generate an XML response."""
    return Response(response_data, content_type='application/xml')


@blueprint.route('/status', methods=['GET'])
def status() -> tuple:
    """Health check endpoint."""
    return xmlify('<status>ok</status>'), HTTP_200_OK


@blueprint.route('/extract', methods=['POST'])
def extract() -> tuple:
    """Handle a request for reference extraction for a POSTed PDF."""
    if 'file' not in request.files:
        return xmlify('<reason>missing file</reason>'), HTTP_400_BAD_REQUEST

    try:
        filepath = handle_upload(request.files['file'])
    except ValueError as e:
        return xmlify('<reason>%s</reason>' % str(e)), HTTP_400_BAD_REQUEST

    try:
        response_data = extract_with_cermine(filepath)
        status = HTTP_200_OK
    except Exception as e:
        response_data = '<reason>cermine failed: %s</reason>' % e
        status = HTTP_500_INTERNAL_SERVER_ERROR
    finally:
        cleanup_upload(filepath)

    return xmlify(response_data), status


def handle_upload(uploaded_file: FileStorage) -> str:
    """Store an uploaded file."""
    filename = secure_filename(uploaded_file.filename)
    if not filename.endswith('.pdf'):
        raise ValueError('Unsupported file type')
    upload_path = current_app.config.get('UPLOAD_PATH', tempfile.mkdtemp())
    stub, ext = os.path.splitext(filename)
    workdir = os.path.join(upload_path, stub)
    if not os.path.exists(workdir):
        os.mkdir(workdir)
    filepath = os.path.join(workdir, filename)
    uploaded_file.save(filepath)
    return filepath


def cleanup_upload(filepath: str) -> None:
    """Remove uploaded file."""
    if os.path.exists(filepath):
        shutil.rmtree(os.path.split(filepath)[0])
    return
