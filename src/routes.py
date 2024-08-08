from flask import Blueprint, url_for, request
from flask_restx import Api, Resource, RestError
from time import sleep

from .service.ai_service import get_dynamic_results

from .utils import response_handler


class CustomApi(Api):
    @property
    def specs_url(self):
        """Monkey patch for HTTPS"""
        scheme = 'http' if '5000' in self.base_url else 'https'
        return url_for(self.endpoint('specs'), _external=True, _scheme=scheme)


blueprint = Blueprint('api', __name__)  # this is swagger documentation
api = CustomApi(blueprint,
                doc='/api/doc/',
                title='Fin Analytics Service REST API',
                version='1.0',
                description='A web service to get the template and stream data to downstream services',
                )


@api.route('/ping', doc=False)
class HealthCheck(Resource):
    @staticmethod
    def get():
        return 'pong'


@api.route('/timeout', doc=False)
class TimeoutTest(Resource):
    def get(self):
        time = request.args.get('time')
        sleep(int(time))
        return 'req finished after ' + time + ' seconds'


@api.route('/analytics/dynamicQueries', methods=['POST'])
class executeDynamicRequests(Resource):
    def post(self):
        try:
            request_body = request.get_json()
            if 'question' not in request_body:
                response = {'message': "Question is empty"}, 400
                return response_handler.formatted(response)
            response = get_dynamic_results(request_body['question'])
            return response_handler.formatted(response)
        except RestError as e:
            response = {'message': str(e.msg)}, 400
            return response_handler.formatted(response)
