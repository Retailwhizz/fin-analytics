import os
from datetime import datetime
from src import create_app, routes
from src.utils.logger import logger
from src.utils.error_handlers import errors_blueprint

app_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

app = create_app()
app.register_blueprint(routes.blueprint)
app.register_blueprint(errors_blueprint)


@app.after_request
def apply_headers(response):
    response.headers["X-App-Runtime"] = app_run_time
    return response


app.app_context().push()
app.logger.disabled = False

if __name__ == '__main__':
    logger.debug('application starting')
    app.run(host='0.0.0.0', port=8050)
