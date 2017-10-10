"""HTTP routes for refextract API."""

import os
from refextract import extract_references_from_file
from flask.json import jsonify
from flask import Blueprint, request, current_app
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
import logging

HTTP_200_OK = 200
HTTP_400_BAD_REQUEST = 400
HTTP_500_INTERNAL_SERVER_ERROR = 500


blueprint = Blueprint('refextract', __name__, url_prefix='/refextract')


def getLogger():
    """Create a logger based on application configuration."""
    default_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
    try:
        log_level = current_app.config.get('LOGLEVEL', logging.INFO)
        log_format = current_app.config.get('LOGFORMAT', default_format)
        log_file = current_app.config.get('LOGFILE')
    except AttributeError:
        log_level = logging.INFO
        log_format = default_format
        log_file = None

    logging.basicConfig(format=log_format)
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    if log_file is not None:
        logger.addHandler(logging.FileHandler(log_file))
    return logger


def handle_upload(uploaded_file: FileStorage) -> str:
    """Store an uploaded file."""
    filename = secure_filename(uploaded_file.filename)
    if not filename.endswith('.pdf'):
        raise ValueError('Unsupported file type')
    filepath = os.path.join(current_app.config['UPLOAD_PATH'], filename)
    uploaded_file.save(filepath)
    return filepath


def cleanup_upload(filepath: str) -> None:
    """Remove uploaded file."""
    if os.path.exists(filepath):
        os.remove(filepath)
    return


@blueprint.route('/status', methods=['GET'])
def status() -> tuple:
    """Health check endpoint."""
    return jsonify({'iam': 'ok'}), HTTP_200_OK


@blueprint.route('/extract', methods=['POST'])
def extract() -> tuple:
    """Handle a request for reference extraction for a POSTed PDF."""
    logger = getLogger()
    if 'file' not in request.files:
        return jsonify({'explanation': 'No file found'}), HTTP_400_BAD_REQUEST

    try:
        filepath = handle_upload(request.files['file'])
    except ValueError as e:
        return jsonify({'explanation': e.msg}), HTTP_400_BAD_REQUEST

    try:
        response_data = extract_references_from_file(filepath)
        status = HTTP_200_OK
    except Exception as e:
        response_data = {'explanation': 'refextract failed: %s' % e.msg}
        status = HTTP_500_INTERNAL_SERVER_ERROR
    finally:
        try:
            cleanup_upload(filepath)
        except IOError as e:
            logger.warning('Could not remove file %s: %s' % filepath, e)

    return jsonify(response_data), status
