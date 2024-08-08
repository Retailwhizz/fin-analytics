from flask import Flask
from .config import Config as config
from .auth.middleware import Middleware
# from .connectors import init_db
from werkzeug.middleware.proxy_fix import ProxyFix


def create_app():
    app = Flask(__name__)
    app.config.from_object(config)
    app.wsgi_app = ProxyFix(app.wsgi_app)
    app.wsgi_app = Middleware(app.wsgi_app)
    # init_db()
    return app
