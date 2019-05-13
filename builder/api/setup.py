'''
Set up Flask server
'''

from flask import Flask, Blueprint
from flask_restful import Api
from flasgger import Swagger

app = Flask(__name__, static_folder='../pack', template_folder='../templates')

api_blueprint = Blueprint('api', __name__, url_prefix='/api')
api = Api(api_blueprint)
app.register_blueprint(api_blueprint)

template = {
    "openapi": "3.0.1",
    "info": {
        "title": "ROBOKOP Builder",
        "description": "An API connecting questions with biomedical knowledge services",
        "contact": {
            "responsibleOrganization": "CoVar Applied Technologies",
            "responsibleDeveloper": "patrick@covar.com",
            "email": "patrick@covar.com",
            "url": "www.covar.com",
        },
        "termsOfService": "<url>",
        "version": "0.0.1"
    },
    "schemes": [
        "http",
        "https"
    ]
}
app.config['SWAGGER'] = {
    'title': 'ROBOKOP Builder API',
    'uiversion': 3
}
swagger_config = {
    "headers": [
    ],
    "specs": [
        {
            "endpoint": 'apispec_1',
            "route": '/builder/spec',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
    "swagger_ui": True,
    "specs_route": "/apidocs/",
    "openapi": "3.0.1"
}
swagger = Swagger(app, template=template, config=swagger_config)