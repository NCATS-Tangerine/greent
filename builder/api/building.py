#!/usr/bin/env python

"""Flask REST API server for builder"""

import sys
import os
import json
import requests
import logging
import yaml
from typing import NamedTuple

import redis
from flask import request
from flask_restful import Resource, reqparse
from neo4j.v1 import GraphDatabase, basic_auth

from greent.annotators import annotator_factory
from builder.api.setup import app, api
from builder.api.tasks import update_kg
import builder.api.logging_config
import greent.node_types as node_types
from greent.synonymization import Synonymizer
import builder.api.definitions
from builder.buildmain import setup
from greent.graph_components import KNode
from greent.util import LoggingUtil
import re

rosetta_config_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "rosetta.yml")
properties_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "conf", "annotation_map.yaml")
predicates_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "conf", "predicates.json")

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class UpdateKG(Resource):
    def post(self):
        """
        Update the cached knowledge graph 
        ---
        tags: [build]
        requestBody:
            name: question
            description: The machine-readable question graph.
            content:
                application/json:
                    schema:
                        $ref: '#/definitions/Question'
            required: true
        responses:
            202:
                description: Update started...
                content:
                    application/json:
                        schema:
                            type: object
                            required:
                            - task id
                            properties:
                                task id:
                                    type: string
                                    description: task ID to poll for KG update status
        """
        task = update_kg.apply_async(args=[request.json])
        logger.info(f"KG update task start with id {task.id}")
        return {'task_id': task.id}, 202

api.add_resource(UpdateKG, '/')

class Synonymize(Resource):
    def post(self, node_id, node_type):
        """
        Return the best identifier for a concept, and its known synonyms
        ---
        tags: [util]
        parameters:
          - in: path
            name: node_id
            description: curie of the node
            schema:
                type: string
            required: true
            default: MONDO:0005737
          - in: path
            name: node_type
            description: type of the node
            schema:
                type: string
            required: true
            default: disease
        responses:
            200:
                description: Synonymized node
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                id:
                                    type: string
                                name:
                                    type: string
                                type:
                                    type: string
                                synonyms:
                                    type: array
                                    items:
                                        type: string
        """
        greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
        sys.path.insert(0, greent_path)
        rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))

        node = KNode(id=node_id, type=node_type, name='')

        try:
            #synonymizer = Synonymizer(rosetta.type_graph.concept_model, rosetta)
            rosetta.synonymizer.synonymize(node)
        except Exception as e:
            logger.error(e)
            return e.message, 500

        result = {
            'id': node.id,
            'name': node.name,
            'type': node.type,
            'synonyms': list(node.synonyms)
        }
        return result, 200

api.add_resource(Synonymize, '/synonymize/<node_id>/<node_type>/')

def rossetta_setup_default():
    greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
    sys.path.insert(0, greent_path)
    rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))
    return rosetta


def normalize_edge_source(knowledge_graph, id_mappings):
    source_map = load_edge_source_json_map()
    edges = knowledge_graph['edges']
    for edge in edges:
        edge['source_id'] = id_mappings.get(edge['source_id'],edge['source_id'])
        edge['target_id'] = id_mappings.get(edge['target_id'], edge['target_id'])
        if 'source_database' in edge:
            if type(edge['source_database']) == type([]):
                edge['source_database'] = [source_map.get(source,source) for source in edge['source_database']]
            else:
                edge['source_database'] = source_map.get(edge['source_database'],edge['source_database'])
    return knowledge_graph
    

def load_edge_source_json_map():
    map_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
    sys.path.insert(0, map_path)
    path = os.path.join(map_path, 'greent','conf','source_map.json')
    with open(path) as f :
        return  json.load(f)


def synonymize_knowledge_graph(knowledge_graph):
    id_mappings = {}
    if 'nodes' in knowledge_graph:
        rosetta = rossetta_setup_default()
        nodes = knowledge_graph['nodes']
        for node in nodes:
            id = ':'.join(re.split(r'\..*:',node['id']))
            # try and make nodes with single type
            nodes = []
            if type(node['type']) == type([]):
                nodes = [KNode(id = id, type = node_type) for node_type in node['type']]
            else:
                nodes = [KNode(id = id, type = node['type'])]
            id_picks = [id]
            for n in nodes:
                rosetta.synonymizer.synonymize(n)
                # if node Id after synonymization is d/t from kg node track change to use that
                if n.id not in id_picks:
                    id_picks.append(n.id)
                if 'equivalent_identifiers' not in node:
                    node['equivalent_identifiers'] = [] 
                node['equivalent_identifiers'].extend([x[0] for x in list(n.synonyms) if x[0] not in node['equivalent_identifiers']])
            id_last_change = id_picks[len(id_picks) - 1]
            id_mappings[node['id']] = id_last_change
            node['id'] = id_last_change
    else: 
        logger.warning('Unable to locate nodes in knowledge graph')
    return knowledge_graph, id_mappings

def synonymize_binding_nodes(answer_set, id_mappings):
    if 'question_graph' not in answer_set:
        logger.warning('No question graph in parameter')
        return answer_set
    for answer in answer_set['answers']:
        node_bindings = answer['node_bindings']
        for node_id in node_bindings:
            curie = node_bindings[node_id] 
            converted = ''
            if type(curie) == type([]):
                converted = [id_mappings.get(c, c) for c in curie]
            else:
                converted = id_mappings.get(curie, curie)
            node_bindings[node_id] = converted
    return answer_set    
        

class NormalizeAnswerSet(Resource):
    def post(self):
        """
        Adds synonmys to node and normalize edge db source for a json blob of answer knowledge graph.
        ---
        tags: [util]
        requestBody:
            name: Answer
            description: The answer graph.
            content:
                application/json:
                    schema:
                        $ref: '#/definitions/Answer'
            required: true
        responses:
            200:
                description: Previous Knowledge graph with nodes synonymized with 'equivalent_identifiers' array field added to the node (if not provided). Edges will contain a new field with 'normalized_edge_source' (string).
        """
        # some sanity checks
        json_blob = request.json
        if 'knowledge_graph' in json_blob and 'nodes' in json_blob['knowledge_graph']:
            json_blob['knowledge_graph'], id_mappings = synonymize_knowledge_graph(json_blob['knowledge_graph'])
            json_blob['knowledge_graph'] = normalize_edge_source(json_blob['knowledge_graph'], id_mappings)
            json_blob = synonymize_binding_nodes(json_blob, id_mappings)
            return json_blob, 200
        return [], 400
api.add_resource(NormalizeAnswerSet, '/normalize')

class Annotator(Resource):
    def get(self, node_id, node_type):
        node = KNode(id= node_id, type= node_type)
        greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
        sys.path.insert(0, greent_path)
        rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))
        rosetta.synonymizer.synonymize(node)
        equivalent_ids = {x[0]:x[1] for x in list(node.synonyms)}
        response = {
            'id': node.id,
            'equivalent_identifiers': equivalent_ids,
            'type': node.type,
            'name': equivalent_ids[node.id]
        }
        try:
            result = annotator_factory.annotate_shortcut(node, rosetta)
            if type(result) == type(None):
                logger.debug(f'No annotator found for {node}')
                return {'error': f'No annotator found for {node}'}
        except Exception as e:
            return {'error': str(e)}, 500
        response.update(node.properties)
        return response, 200
api.add_resource(Annotator, '/annotate/<node_id>/<node_type>/')


class TaskStatus(Resource):
    def get(self, task_id):
        """
        Get the status of a task
        ---
        tags: [tasks]
        parameters:
          - in: path
            name: task_id
            description: "ID of the task"
            schema:
                type: string
            required: true
        responses:
            200:
                description: Task status
                content:
                    application/json:
                        schema:
                            type: object
                            required:
                            - task-id
                            - state
                            - result
                            properties:
                                task_id:
                                    type: string
                                status:
                                    type: string
                                    description: Short task status
                                result:
                                    type: ???
                                    description: Result of completed task OR intermediate status message
                                traceback:
                                    type: string
                                    description: Traceback, in case of task failure
        """

        r = redis.Redis(
            host=os.environ['RESULTS_HOST'],
            port=os.environ['RESULTS_PORT'],
            db=os.environ['BUILDER_RESULTS_DB'],
            password=os.environ['RESULTS_PASSWORD']
        )
        value = r.get(f'celery-task-meta-{task_id}')
        if value is None:
            return 'Task not found', 404
        result = json.loads(value)
        return result, 200

api.add_resource(TaskStatus, '/task/<task_id>')

class TaskLog(Resource):
    def get(self, task_id):
        """
        Get activity log for a task
        ---
        tags: [util]
        parameters:
          - in: path
            name: task_id
            description: ID of task
            schema:
                type: string
            required: true
        responses:
            200:
                description: text
        """

        task_log_file = os.path.join(os.environ['ROBOKOP_HOME'], 'logs','builder_task_logs', f'{task_id}.log')
        if os.path.isfile(task_log_file):
            with open(task_log_file, 'r') as log_file:
                log_contents = log_file.read()
            return log_contents, 200
        else:
            return 'Task ID not found', 404

api.add_resource(TaskLog, '/task/<task_id>/log')

class Operations(Resource):
    def get(self):
        """
        Get a JSON list of all edges in the type graph
        ---
        tags: [util]
        responses:
            200:
                description: Operations
                content:
                    application/json:
                        schema:
                            type: array
                            items:
                                type: string
        """
        with open(rosetta_config_file, 'r') as stream:
            config = yaml.load(stream)
        
        operators = config["@operators"]

        return operators

api.add_resource(Operations, '/operations')

class Predicates(Resource):
    def get(self):
        """
        Get a JSON object of predicates for a source-target pair
        ---
        tags: [util]
        responses:
            200:
                description: Operations
                content:
                    application/json:
                        schema:
                            type: object
        """
        with open(predicates_file, 'r') as f:
            predicate_conf = json.load(f)

        return predicate_conf

    def post(self):
        """
        Force update of source-target predicate list from neo4j database
        ---
        tags: [util]
        responses:
            200:
                description: "Here's your updated source-target predicate list"
                content:
                    application/json:
                        schema:
                            type: object
            400:
                description: "Something went wrong. Old predicate list will be retained"
                content:
                    text/plain:
                        schema:
                            type: string
        """
        # Grab all predicates from neo4j - Array of lists of form:
        # ['predicate_name',[source type list], [target type list]]
        driver = GraphDatabase.driver(f"bolt://{os.environ['NEO4J_HOST']}:{os.environ['NEO4J_BOLT_PORT']}",
            auth=basic_auth("neo4j", os.environ['NEO4J_PASSWORD']))
        with driver.session() as session:
            result = session.run('match (a)-[x]-(b) return distinct type(x), labels(a), labels(b)')
            records = [list(r) for r in result]

        # Reformat predicate list into a dict of dicts with first key as
        # source_type, 2nd key as target_type, and value as a list of all 
        # supported predicates for the source-target pairing
        type_black_list = ['Concept', 'named_thing']
        pred_dict = dict()
        for row in records:
            predicate = row[0]
            sourceTypes = [r for r in row[1] if r not in type_black_list]
            targetTypes = [r for r in row[2] if r not in type_black_list]

            for s in sourceTypes:
                for t in targetTypes:
                    if s not in pred_dict:
                        pred_dict[s] = dict()
                    if t not in pred_dict[s]:
                        pred_dict[s][t] = []
                    pred_dict[s][t].append(predicate)

        with open(predicates_file, 'w') as f:
            json.dump(pred_dict, f, indent=2)

        return pred_dict, 201

api.add_resource(Predicates, '/predicates')


class Connections(Resource):
    def get(self):
        """
        Get a simplified list of all edges in the type graph
        ---
        
        tags: [util]
        responses:
            200:
                description: Operations
                content:
                    application/json:
                        schema:
                            type: array
                            items:
                                type: string
        """
        with open(rosetta_config_file, 'r') as stream:
            config = yaml.load(stream)
        
        operations = config["@operators"]

        s = []
        for start in operations:
            for stop in operations[start]:
                s.append(f"{start} -> {stop}")

        return s

api.add_resource(Connections, '/connections')

class Properties(Resource):
    def get(self):
        """
        Get a list of all node properties that may be in the graph
        ---
        tags: [util]
        responses:
            200:
                description: Properties
                content:
                    application/json:
        """
        with open(properties_file, 'r') as stream:
            properties = yaml.load(stream)

        return properties

api.add_resource(Properties, '/properties')


class Concepts(Resource):
    def get(self):
        """
        Get known biomedical concepts
        ---
        tags: [util]
        responses:
            200:
                description: Concepts
                content:
                    application/json:
                        schema:
                            type: array
                            items:
                                type: string
        """
        concepts = list(node_types.node_types - {'unspecified'})
        return concepts

api.add_resource(Concepts, '/concepts')

if __name__ == '__main__':

    # Get host and port from environmental variables
    server_host = '0.0.0.0'
    server_port = int(os.environ['BUILDER_PORT'])

    app.run(host=server_host,\
        port=server_port,\
        debug=True,\
        use_reloader=True)
