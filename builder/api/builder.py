#!/usr/bin/env python

"""Flask REST API server for builder"""

import os
import json
import requests

from flask import request
from flask_restful import Resource, reqparse
from flasgger import Swagger

from setup import app, api, swagger
import builder.question
from builder.api.tasks import update_kg

class UpdateKG(Resource):
    def post(self):
        """
        Update the cached knowledge graph 
        ---
        parameters:
          - in: body
            name: question
            description: The machine-readable question graph.
            schema:
                $ref: '#/definitions/Question'
            required: true
        responses:
            202:
                description: Update started...
                schema:
                    type: object
                    required:
                      - poll
                    properties:
                        poll:
                            type: string
                            description: URL to poll for KG update status
        """
        # replace parameters with this when OAS 3.0 is fully supported by Swagger UI
        # https://github.com/swagger-api/swagger-ui/issues/3641
        """
        requestBody:
            description: The machine-readable question graph.
            required: true
            content:
                application/json:
                    schema:
                        $ref: '#/definitions/Question'
        """
        task = update_kg.apply(args=[request.json])
        polling_url = f'http://{os.environ["FLOWER_ADDRESS"]}:{os.environ["FLOWER_PORT"]}/api/task/result/{task.id}'
        return {'poll': polling_url}, 202

api.add_resource(UpdateKG, '/')

class TaskStatus(Resource):
    def get(self, task_id):
        """
        Update the cached knowledge graph 
        ---
        parameters:
          - in: path
            name: task_id
            description: ID of the task
            type: string
            required: true
        responses:
            200:
                description: Task status
                schema:
                    type: object
                    required:
                      - task-id
                      - state
                      - result
                    properties:
                        task-id:
                            type: string
                            description: Task ID
                        state:
                            type: string
                            description: Short task status
                        result:
                            type: string
                            description: Result of completed task OR intermediate status message
                        traceback:
                            type: string
                            description: Traceback, in case of task failure
        """
        polling_url = f'http://{os.environ["FLOWER_ADDRESS"]}:{os.environ["FLOWER_PORT"]}/api/task/result/{task_id}'
        response = requests.get(polling_url, auth=(os.environ['FLOWER_USER'], os.environ['FLOWER_PASSWORD']))
        return response.json(), 200

api.add_resource(TaskStatus, '/task/<task_id>')

if __name__ == '__main__':

    # Get host and port from environmental variables
    server_host = os.environ['ROBOKOP_HOST']
    server_port = 6011

    app.run(host=server_host,\
        port=server_port,\
        debug=True,\
        use_reloader=True)
