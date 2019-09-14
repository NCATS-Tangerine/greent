#!/usr/bin/env python

"""Flask REST API server for builder"""

from collections import defaultdict
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
import builder.api.logging_config
import greent.node_types as node_types
from greent.synonymization import Synonymizer
import builder.api.definitions
from builder.buildmain import setup
from greent.graph_components import KNode
from greent.util import LoggingUtil
import re
from builder.question import Question

rosetta_config_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "rosetta.yml")
properties_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "conf", "annotation_map.yaml")
source_licenses = os.path.join(os.path.dirname(__file__), "..", "..", "source_licenses.yaml")
predicates_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "conf", "predicates.json")
node_props_file = os.path.join(os.path.dirname(__file__), "..", "..", "greent", "conf", "properties.json")

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)


def synonymize(node_id, node_type=node_types.NAMED_THING):
    """Return synonymized node."""
    greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
    sys.path.insert(0, greent_path)
    rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))
    node = KNode(id=node_id, type=node_type, name='')
    rosetta.synonymizer.synonymize(node)
    result = {
        'id': node.id,
        'name': node.name,
        'type': node.type,
        'synonyms': list(node.synonyms)
    }
    return result


class Synonymize(Resource):
    def post(self, node_id):
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
        try:
            result = synonymize(node_id)
        except Exception as e:
            logger.error(e)
            return e.message, 500
        return result, 200
api.add_resource(Synonymize, '/synonymize/<node_id>/')

class SynonymizeDeprecated(Resource):
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
        deprecated: true
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
        #TODO: remove this 
        try:
            result = synonymize(node_id, node_type)
        except Exception as e:
            logger.error(e)
            return e.message, 500
        return result, 200
api.add_resource(SynonymizeDeprecated, '/synonymize/<node_id>/<node_type>/')

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
        Adds synonyms to node and normalize edge db source for a json blob of answer knowledge graph.
        ---
        tags: [util]
        requestBody:
            name: Message
            description: A message.
            content:
                application/json:
                    schema:
                        $ref: '#/definitions/Message'
                    example:
                        knowledge_graph:
                            nodes:
                              - id: MONDO:0005737
                            edges: []
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
api.add_resource(NormalizeAnswerSet, '/normalize/')

class Annotator(Resource):
    def get(self, node_id, node_type):
        """
        Returns annotation for a node.
        ---
        tags: [util]
        parameters:
          - in: path
            name: node_id
            description: "curie of the node"
            schema:
                type: string
            default: MONDO:0005737
            required: true
          - in: path
            name: node_type
            description: "Biolink type name for the curie, eg. providing chemical_substance here will make sure the annotation is done as a chemical substance."
            schema:
                type: string
            default: disease
            required: true
        responses:
            200:
                description: Node annotations
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                id:
                                    type: string
                                equivalent_identifiers:
                                    type: array
                                    items:
                                        type: string
        """
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

api.add_resource(Operations, '/operations/')

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
            result = session.run("""
                match (a)-[x]->(b) with
                filter(la in labels(a) where not la in ['named_thing', 'Concept']) as las,
                filter(lb in labels(b) where not lb in ['named_thing', 'Concept']) as lbs,
                type(x) as predicate
                unwind las as la unwind lbs as lb
                return distinct predicate, la, lb
            """)
            records = [list(r) for r in result]

        # Reformat predicate list into a dict of dicts with first key as
        # source_type, 2nd key as target_type, and value as a list of all 
        # supported predicates for the source-target pairing
        pred_dict = defaultdict(lambda: defaultdict(list))
        for row in records:
            predicate = row[0]
            source_type = row[1]
            target_type = row[2]
            logger.debug(predicate, source_type, target_type)
            pred_dict[source_type][target_type].append(predicate)

        with open(predicates_file, 'w') as f:
            json.dump(pred_dict, f, indent=2)

        return pred_dict, 201

api.add_resource(Predicates, '/predicates/')

class NodeProperties(Resource):
    def get(self):
        """
        Get a JSON object of properties for each node type
        ---
        tags: [util]
        responses:
            200:
                description: Operations
                content:
                    application/json:
                        schema:
                            type: object
                            additionalProperties:
                                type: array
                                items:
                                    type: string
        """
        with open(node_props_file, 'r') as f:
            prop_dict = json.load(f)

        return prop_dict

    def post(self):
        """
        Force update of node-type property list from neo4j database
        ---
        tags: [util]
        responses:
            200:
                description: Node-type property list
                content:
                    application/json:
                        schema:
                            type: object
                            additionalProperties:
                                type: array
                                items:
                                    type: string
            400:
                description: "Something went wrong. Old node-type properties list will be retained"
                content:
                    text/plain:
                        schema:
                            type: string
        """
        driver = GraphDatabase.driver(
            f"bolt://{os.environ['NEO4J_HOST']}:{os.environ['NEO4J_BOLT_PORT']}",
            auth=basic_auth("neo4j", os.environ['NEO4J_PASSWORD'])
        )
        with driver.session() as session:
            result = session.run("MATCH (n) WITH labels(n) AS types, apoc.map.sortedProperties(apoc.meta.types(n)) as props UNWIND types AS type UNWIND props AS prop RETURN type, collect(DISTINCT apoc.text.join(prop, ':')) AS props")
            records = [list(r) for r in result]

        type_blacklist = ['Concept', 'named_thing', 'Type']
        prop_blacklist = ['id:STRING', 'name:STRING', 'equivalent_identifiers:STRING[]']
        prop_dict = dict()
        for row in records:
            node_type = row[0]
            if node_type in type_blacklist:
                continue
            properties = [r for r in row[1] if r not in prop_blacklist]
            prop_dict[node_type] = properties

        with open(node_props_file, 'w') as f:
            json.dump(prop_dict, f, indent=2)

        return prop_dict, 201

api.add_resource(NodeProperties, '/node_properties/')


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

api.add_resource(Connections, '/connections/')

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
                        schema:
                            type: object
                            additionalProperties:
                                type: object
                                properties:
                                    node_type:
                                        type: string
                                    prefixes:
                                        type: array
                                        items:
                                            type: string
                                additionalProperties:
                                    type: object
                                    properties:
                                        url:
                                            type: string
                                        keys:
                                            type: object
                                            additionalProperties:
                                                type: object
                                                properties:
                                                    source:
                                                        type: string
                                                    data_type:
                                                        type: string
        """
        with open(properties_file, 'r') as stream:
            properties = yaml.load(stream)

        return properties

api.add_resource(Properties, '/properties/')


class SourceLicenses(Resource):
    def get(self):
        """
        Get a list of all source licences from knowledge sources
        ---
        tags: [util]
        responses:
            200:
                description: Source Licenses
                content:
                    application/json:
                        schema:
                            type: array
                            items:
                                type: object
                                properties:
                                    name:
                                        type: string
                                        example: Drugbank
                                    url:
                                        type: string
                                        example: https://www.drugbank.ca/
                                    license:
                                        type: string
                                        example: CC-BY-NC-4.0
                                    license_url:
                                        type: string
                                        example: https://www.drugbank.ca/releases/latest
                                    citation_url:
                                        type: string
                                        example: https://www.drugbank.ca/about
        """
        with open(source_licenses, 'r') as stream:
            sources = yaml.load(stream)

        return sources

api.add_resource(SourceLicenses, '/sourceLicenses/')


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

api.add_resource(Concepts, '/concepts/')


if __name__ == '__main__':

    # Get host and port from environmental variables
    server_host = '0.0.0.0'
    server_port = int(os.environ['BUILDER_PORT'])

    app.run(host=server_host,\
        port=server_port,\
        debug=True,\
        use_reloader=True)
