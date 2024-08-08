from .errors import RestError, codes


def formatted(response):
    if isinstance(response, RestError):
        message = [str(x) for x in response.args]
        status_code = int(message[0]) if str.isnumeric(message[0]) else 500
        json_response = {
            'type': response.__class__.__name__,
            'message': message[1] if (len(message) > 1 and message[1]) else (
                codes[status_code] if status_code in codes else "Internal Server Error")
        }
    elif isinstance(response, Exception):
        status_code = 500
        json_response = {
            'type': 'UnhandledException',
            'message': 'An unexpected error has occurred.'
        }
    elif isinstance(response, tuple):
        status_code = response[1]
        json_response = response[0]
    else:
        status_code = 200
        json_response = response

    modified_response = {
        'status': True,
        'response': {}
    }
    status_code = status_code if status_code else 200
    if status_code in range(200, 299):
        modified_response['response']['result'] = json_response
    else:
        modified_response['response']['error'] = json_response
        modified_response['status'] = False

    return modified_response, status_code
