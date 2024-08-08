from flask import Blueprint, jsonify
from src.utils.logger import logger
from src.utils import response_handler

errors_blueprint = Blueprint('errors', __name__)

logger = logger.getChild(__name__)


@errors_blueprint.app_errorhandler(Exception)
def handle_unexpected_error(error):
    logger.error(error)
    response, status_code = response_handler.formatted(error)
    return jsonify(response), status_code
