from starlette.middleware.wsgi import WSGIMiddleware

from arquivio.web.app import app as flask_app

app = WSGIMiddleware(flask_app)
