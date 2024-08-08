import os, json, re
from werkzeug.wrappers import Request, Response
from ..config import Config as config
from src.utils.logger import logger
from src.utils import response_handler


logger = logger.getChild(__name__)

auth_error = {'message': 'Authentication failed.', 'type': 'AuthError'}
formatted_response, status_code = response_handler.formatted((auth_error, 401))
res = Response(json.dumps(formatted_response), mimetype='application/json', status=401)

no_auth_required = dict(
    exact=['/', '/api/doc/', '/swagger.json', '/ping'],
    pattern=['^/swaggerui/*']
)


class Middleware:
    def __init__(self, app):
        self.app = app
        self.auth_token = 123

    def __call__(self, environ, start_response):
        request = Request(environ)
        logger.debug('%s %s', request.method, request.path)
        return self.app(environ, start_response)
        #
        # if os.getenv('NO_AUTH') == 'true':
        #     return self.app(environ, start_response)
        # try:
        #     for key in no_auth_required['pattern']:
        #         if re.match(key, request.path):
        #             return self.app(environ, start_response)
        #
        #     if request.path in no_auth_required['exact']:
        #         return self.app(environ, start_response)
        #
        #     if request.headers['x-access-token'] == self.auth_token:
        #         return self.app(environ, start_response)
        #     else:
        #         return res(environ, start_response)
        # except KeyError as e:
        #     logger.error('Key error for path %s,  key:%s', request.path, e)
        #     return res(environ, start_response)
