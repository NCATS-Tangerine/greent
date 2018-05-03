#!/usr/bin/env python

"""Flask REST API server for builder"""

import os
import json
import requests
import logging

from flask import request
from flask_restful import Resource, reqparse
from flasgger import Swagger

from setup import app, api, swagger
# from builder.question import Question
from builder.api.tasks import update_kg
import builder.api.logging_config

from builder.util import FromDictMixin
@swagger.definition('Node')
class Node(FromDictMixin):
    """
    Node Object
    ---
    schema:
        id: Node
        required:
            - id
        properties:
            id:
                type: string
                required: true
            type:
                type: string
            identifiers:
                type: array
                items:
                    type: string
                default: []
    """
    def __init__(self, *args, **kwargs):
        self.id = None
        self.type = None
        self.identifiers = []

        super().__init__(*args, **kwargs)

    def dump(self):
        return {**vars(self)}

@swagger.definition('Edge')
class Edge(FromDictMixin):
    """
    Edge Object
    ---
    schema:
        id: Edge
        required:
            - start
            - end
        properties:
            start:
                type: string
            end:
                type: string
            min_length:
                type: integer
                default: 1
            max_length:
                type: integer
                default: 1
    """
    def __init__(self, *args, **kwargs):
        self.start = None
        self.end = None
        self.min_length = 1
        self.max_length = 1

        super().__init__(*args, **kwargs)

    def dump(self):
        return {**vars(self)}

@swagger.definition('Question')
class Question(FromDictMixin):
    """
    Question Object
    ---
    schema:
        id: Question
        required:
          - nodes
          - edges
        properties:
            nodes:
                type: array
                items:
                    $ref: '#/definitions/Node'
            edges:
                type: array
                items:
                    $ref: '#/definitions/Edge'
        example:
            nodes:
              - id: 0
                type: disease
                identifiers: ["MONDO:0008753"]
              - id: 1
                type: gene
              - id: 2
                type: genetic_condition
            edges:
              - start: 0
                end: 1
              - start: 1
                end: 2
    """

    def __init__(self, *args, **kwargs):
        '''
        keyword arguments: id, user, notes, natural_question, nodes, edges
        q = Question(kw0=value, ...)
        q = Question(struct, ...)
        '''
        # initialize all properties
        self.nodes = [] # list of nodes
        self.edges = [] # list of edges

        super().__init__(*args, **kwargs)

    def preprocess(self, key, value):
        if key == 'nodes':
            return [Node(n) for n in value]
        elif key == 'edges':
            return [Edge(e) for e in value]

    def dump(self):
        return {**vars(self)}

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
        # replace `parameters`` with this when OAS 3.0 is fully supported by Swagger UI
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
        logger = logging.getLogger('builder')
        logger.info("updating kg...")
        task = update_kg.apply(args=[request.json])
        polling_url = f'http://{os.environ["FLOWER_ADDRESS"]}:{os.environ["BUILDER_FLOWER_PORT"]}/api/task/result/{task.id}'
        return {'poll': polling_url}, 202

api.add_resource(UpdateKG, '/')

class TaskStatus(Resource):
    def get(self, task_id):
        """
        Get the status of a task
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
        polling_url = f'http://{os.environ["FLOWER_ADDRESS"]}:{os.environ["BUILDER_FLOWER_PORT"]}/api/task/result/{task_id}'
        response = requests.get(polling_url, auth=(os.environ['FLOWER_USER'], os.environ['FLOWER_PASSWORD']))
        return response.json(), 200

api.add_resource(TaskStatus, '/task/<task_id>')

if __name__ == '__main__':

    # Get host and port from environmental variables
    server_host = os.environ['ROBOKOP_HOST']
    server_port = int(os.environ['ROBOKOP_BUILDER_PORT'])

    app.run(host=server_host,\
        port=server_port,\
        debug=True,\
        use_reloader=True)
