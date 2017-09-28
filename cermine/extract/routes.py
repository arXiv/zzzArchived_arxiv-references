"""HTTP routes for refextract API."""

import os
from flask.json import jsonify
from flask import Blueprint, request, current_app, Response
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
import logging
import subprocess
import shlex
import tempfile

HTTP_200_OK = 200
HTTP_400_BAD_REQUEST = 400
HTTP_500_INTERNAL_SERVER_ERROR = 500


blueprint = Blueprint('cermine', __name__, url_prefix='/cermine')


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


@blueprint.route('/status', methods=['GET'])
def status() -> tuple:
    """Health check endpoint."""
    return xmlify('<status>ok</status>'), HTTP_200_OK


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


def xmlify(response_data: dict) -> Response:
    """Generate an XML response."""
    return Response(response_data, content_type='application/xml')


def extract_with_cermine(filepath: str) -> str:
    logger = getLogger()
    basepath, filename = os.path.split(filepath)
    stub, ext = os.path.splitext(os.path.basename(filename))

    cmd = shlex.split("java -cp /opt/cermine/cermine-impl.jar"
                      " pl.edu.icm.cermine.ContentExtractor"
                      " -path %s" % basepath)

    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.debug('%s: Cermine exit code: %i' % (filename, r.returncode))
        logger.debug('%s: Cermine stdout: %s' % (filename, r.stdout))
        logger.debug('%s: Cermine stderr: %s' % (filename, r.stderr))
    except subprocess.CalledProcessError as e:
        raise RuntimeError('CERMINE failed: %s' % filename) from e

    outpath = os.path.join(basepath, '{}.cermxml'.format(stub))
    if not os.path.exists(outpath):
        raise FileNotFoundError('%s not found, expected output' % outpath)
    try:
        with open(outpath, 'rb') as f:
            result = f.read()
    except Exception as e:
        raise IOError('Could not read Cermine output at %s: %s' %
                      (outpath, e)) from e
    return result


@blueprint.route('/', methods=['POST'])
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
        response_data = extract_with_cermine(filepath)
        status = HTTP_200_OK
    except Exception as e:
        response_data = '<explanation>cermine failed: %s</explanation>' % e
        status = HTTP_500_INTERNAL_SERVER_ERROR
    finally:
        try:
            cleanup_upload(filepath)
        except IOError as e:
            logger.warning('Could not remove file %s: %s' % filepath, e)

    return xmlify(response_data), status
