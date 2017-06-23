from flask.views import MethodView
from flask.json import jsonify
from flask import request, url_for, Blueprint

from reflink.controllers import references, pdf
from reflink.types import ViewResponseData

blueprint = Blueprint('reflink_api', __name__, url_prefix='/api')


@blueprint.route('/references/<string:document_id>', methods=['GET'])
def get_reference_metadata(document_id: str) -> ViewResponseData:
    response, status = references.ReferenceMetadataController().get(document_id)
    return jsonify(response), status


@blueprint.route('/pdf/<string:document_id>', methods=['GET'])
def get_pdf_location(document_id: str) -> ViewResponseData:
    response, status = pdf.PDFController().get(document_id)
    return jsonify(response), status
