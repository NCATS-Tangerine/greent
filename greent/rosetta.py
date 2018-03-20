import argparse
import logging
import json
import operator
import os
import pytest
import re
import sys
import traceback
import yaml
import requests_cache
from requests_cache.backends.redis import RedisCache
import redis
import pickle

from collections import defaultdict
from greent.transreg import TranslatorRegistry
from greent.identifiers import Identifiers
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import Text
from greent.util import DataStructure
from greent.graph import TypeGraph
from greent.graph import Operator
from greent.graph import Frame
from greent.graph_components import KNode, KEdge, elements_to_json
from greent.synonymization import Synonymizer
from neo4jrestclient.exceptions import StatusException

logger = LoggingUtil.init_logging(__file__, level=logging.INFO)

class CacheSerializer:
    """ Generic serializer. """
    def __init__(self):
        pass
class PickleSerializer(CacheSerializer):
    """ Use Python's default serialization. """
    def __init__(self):
        pass
    def dumps(self, obj):
        return pickle.dumps (obj)
    def loads(self, str):
        return pickle.loads (str)

class Cache:
    """ Cache objects by various means. """
    def __init__(self, cache_path="cache",
                 serializer=PickleSerializer,
                 redis_host="localhost", redis_port=6379,
                 enabled=True):
        """ Connect to cache. """
        self.enabled = enabled
        try:
            self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=0)
            self.redis.get ('x')
        except:
            self.redis = None
            logger.error ("Failed to connect to redis. Is the server running?")
        self.cache_path = cache_path
        if not os.path.exists (self.cache_path):
            os.makedirs (self.cache_path)
        self.cache = {}
        self.serializer = serializer ()
    def get(self, key):
        result = None
        if self.enabled:
            if self.redis:
                rec = self.redis.get (key)
                result = self.serializer.loads (rec) if rec else None
            elif key in self.cache:
                result = self.cache[key]
            else:
                path = os.path.join (self.cache_path, key)
                if os.path.exists (path):
                    with open(path, 'rb') as stream:
                        result = self.serializer.loads (stream.read ())
                        self.cache[key] = result
        return result
    def set(self, key, value):
        if self.enabled:
            if self.redis:
                if value:
                    self.redis.set (key, self.serializer.dumps (value))
            else:
                self.cache[key] = value
                path = os.path.join (self.cache_path, key)
                with open(path, 'wb') as stream:
                    stream.write (self.serializer.dumps (value))
    def close (self):
        self.cache.close ()
        
class Rosetta:
    """ Rosetta's translates between semantic domains generically and automatically.
    Based on a configuration file, it builds a directed graph where types are nodes.
    Types are concepts from a model. Edges are annotated with the names of operators used
    to transition between the connected types. The engine can then accept requests to 
    translate a term from one domain to another. It does this by collecting transitions
    from the graph and executing the list of transitions. """

    def __init__(self, greentConf="greent.conf",
                 config_file=os.path.join(os.path.dirname(__file__), "rosetta.yml"),
                 override={},
                 delete_type_graph=False,
                 init_db=False,
                 redis_host="localhost",
                 redis_port=6379,
                 debug=False):

        """ The constructor loads the config file an prepares the type graph. If the delete_type_graph 
        flag is true, the graph is deleted entirely. If the init_db flag is true, the type_graph will
        be loaded from the config file. """
        """ Load the config file and set up a DiGraph representing the types we know 
        about and how to transition between them. """
        from greent.core import GreenT
        self.debug = False
        self.cache_path = 'rosetta_cache'

        logger.debug("-- rosetta init.")
        self.core = GreenT(config=greentConf, override=override)

        """ Load configuration. """
        with open(config_file, 'r') as stream:
            self.config = yaml.load(stream)
        #self.concepts = self.config["@concepts"]
        self.operators = self.config["@operators"]

        self.cache = Cache ()

        """ Initialize type graph. """
        self.type_graph = TypeGraph(self.core.service_context, debug=debug)
        self.synonymizer = Synonymizer( self.type_graph.concept_model, self.core )

        """ Merge identifiers.org vocabulary into Rosetta voab. """
        self.identifiers = Identifiers ()
        if delete_type_graph:
            logger.debug("--Deleting type graph")
            self.type_graph.delete_all()

        if init_db:
            """ Initialize type graph metadata. """
            for k, v in self.identifiers.vocab.items():
                if isinstance(v, str):
                    self.type_graph.find_or_create(k, v)
            self.configure_local_operators ()
            #self.configure_translator_registry ()
            
    def configure_local_operators (self):
        logger.debug ("Configure operators in the Rosetta config.")
        for a_concept, transition_list in self.operators.items ():
            for b_concept, transitions in transition_list.items ():
                for transition in transitions:
                    link = transition['link']
                    op   = transition['op']
                    self.create_concept_transition (a_concept, b_concept, link, op)
                    
    def configure_translator_registry (self):
        logger.debug ("Configure operators derived from the Translator Registry.")
        self.core.translator_registry = TranslatorRegistry(self.core.service_context)
        subscriptions = self.core.translator_registry.get_subscriptions()
        registrations = defaultdict(list)
        skip_patterns = list(map (lambda v : re.compile (v),
                              self.config.get ('@translator-registry',{}).get('skip_list', [])))     
        for sub in subscriptions:
            in_concept = sub.in_concept
            out_concept = sub.out_concept
            op = f"translator_registry.{sub.op}"
            key = f"{in_concept}-{out_concept}-{op}"
            link = sub.predicate if sub.predicate else "unknown"
            link = link.upper()
            if any([ p.match (sub.op) for p in skip_patterns ]):
                logger.debug (f"==> Skipping registration of translator API {sub.op} based on configuration setting.")
                continue
            if key in registrations:
                continue
            registrations [key] = sub
            if not in_concept:
                logger.debug(f"Unable to find in concept for {sub}")
            elif not out_concept:
                logger.debug(f"Unable to find out concept for {sub}")
            else:
                if link and op:
                    self.create_concept_transition (in_concept, out_concept, link, op)

    def create_concept_transition (self, a_concept, b_concept, link, op):
        """ Create a link between two concepts in the type graph. """
        logger.debug ("  -+ {} {} link: {} op: {}".format(a_concept, b_concept, link, op))
        try:
            self.type_graph.add_concepts_edge(a_concept, b_concept, predicate=link, op=op)
        except StatusException:
            logger.error(f"Failed to create edge from {a_concept} to {b_concept} with link {link} and op {op}")
            
    def terminate(self, d):
        for k, v in d.items():
            if isinstance(v, str) and not v.endswith("/"):
                d[k] = "{0}/".format(v)

    def unterminate(self, text):
        return text[:-1] if text.endswith('/') else text

    def get_ops(self, names):
        """ Dynamically locate python methods corresponding to names configured for semantic links. """
        return operator.attrgetter(names)(self.core) if isinstance(names, str) else [
            operator.attrgetter(n)(self.core) for n in names]

    def log_debug(self, text, cycle=0, if_empty=False):
        if cycle < 3:
            if (text and len(text) > 0) or if_empty:
                logger.debug("{}".format(text))

    def graph(self, next_nodes, query):
        """ Given a set of starting nodes and a query, execute the query to get a set of paths.
        Each path reflects a set of transitions from the starting tokens through the graph.
        Each path is then executed and the resulting links and nodes returned. """
        programs = self.type_graph.get_transitions(query)
        result = []
        for program in programs:
            result += self.graph_inner(next_nodes, program)
        return result

    def graph_inner(self, next_nodes, program):
        #=import json
        if not program or len(program) == 0:
            return []
        logger.info (program)
        primed = [{'collector': next_nodes}] + program
        linked_result = []
        for index, level in enumerate(program):
            logger.debug("--Executing level: {0}".format(level))
            operators = level['ops']
            collector = level['collector']
            for edge_node in primed[index]['collector']:
                for operator in operators:
                    op = self.get_ops(operator['op'])
                    try:
                        results = None
                        log_text = "  -- {0}({1})".format(operator['op'], edge_node[1].identifier)
                        source_node = edge_node[1]
                        with requests_cache.enabled("rosetta_cache"):
                            results = op(source_node)
                        for r in results:
                            print (f"--- result --- {r}")
                            edge = r[0]
                            if isinstance(edge, KEdge):
                                edge.predicate = operator['link']
                                edge.source_node = source_node
                                self.synonymizer.synonymize(r[1])
                                edge.target_node = r[1]
                                linked_result.append(edge)
                        logger.debug("{0} => {1}".format(log_text, Text.short(results)))
                        for r in results:
                            if index < len(program) - 1:
                                if not r[1].identifier.startswith(program[index + 1]['node_type']):
                                    logger.debug(
                                        "Operator {0} wired to return type: {1} returned node with id: {2}".format(
                                            operator, program[index + 1]['node_type'], r[1].identifier))
                        collector += results
                    except Exception as e:
                        traceback.print_exc()
                        logger.error("Error invoking> {0}".format(log_text))
        return linked_result

    def construct_knowledge_graph (self, inputs, query):
        programs = self.type_graph.get_knowledge_map_programs(query)
        results = []
        for program in programs:
            print (f" program --**-->> {program}")
            results += self.execute_knowledge_graph_program (inputs, program)
        return results
    
    def execute_knowledge_graph_program (self, inputs, program):
        """ Construct a knowledge graph given a set of input nodes and a program - a list
        of frames, each of which contains the name of a concept, a collector containing a list of edges and
        nodes where all target nodes are instances of the frame's concept, and a list of operations for 
        transitioning from one frame's concept space to the next frames.

        This method assumes a linear path.
        """

        """ Convert inputs to be structured like edges-and-nodes returned by a previous services. """
        next_nodes = { key : [ (None, KNode(val, key)) for val in val_list ] for key, val_list in inputs.items () }
        logger.debug (f"inputs: {next_nodes}")

        """ Validated the input program. """
        if len(program) == 0:
            logger.info (f"No program found for {query}")
            return result
        logger.info (f"program> {program}")
        result = []
        threshold = 50000000
        
        """ Each frame's name is a concept. We use the top frame's as a key to index the arguments. """
        top_frame = program[0]
        inputs = next_nodes[top_frame.name]
        for i in inputs:
            self.synonymizer.synonymize(i[1])
            
        """ Stack is the overall executable. We prepend a base frame with a collector primed with input arguments. """
        stack = [ Frame (collector=inputs) ] + program

        """ Execute the program frame by frame. """
        for index, frame in enumerate(program):
            #logger.debug (f"--inputs: {stack[index].collector}")
            for k, o in frame.ops.items ():                
                logger.debug(f"  -- frame-index--> {frame} {index} {k}=>{o.op}")

            """ Process each node in the collector. """
            index = 0
            for edge, source_node in stack[index].collector:
                '''
                if index > threshold:
                    break
                index = index + 1
                '''
                """ Process each operator in the frame. """
                for op_name, operator in frame.ops.items ():

                    """ Generate a cache key. """
                    key =  f"{frame.name}->{operator.op}({source_node.identifier})"
                    try:
                        logger.debug (f"--op: {key}")

                        """ Load the object from cache. """
                        response = self.cache.get (key)
                        if not response:
                            
                            """ Invoke the knowledge source with the given input. """
                            op = self.get_ops(operator.op)
                            response = op(source_node)
                            for edge, node in response:
                                
                                """ Process the edge adding metadata. """
                                if isinstance(edge, KEdge):
                                    edge.predicate = operator.predicate
                                    edge.source_node = source_node
                                    self.synonymizer.synonymize(node)
                                    edge.target_node = node

                                """ Validate the id space of the returned data maps to the target concept. """
                                if index < len(program) - 1:
                                    target_concept_name = program[index + 1].name
                                    prefixes = self.type_graph.concept_model.get (target_concept_name).id_prefixes
                                    valid = any([ node.identifier.upper().startswith(p.upper()) for p in prefixes])
                                    if not valid:
                                        logger.debug(
                                            f"Operator {operator} wired to type: {concept_name} returned node with id: {node.identifier}")

                            """ Cache the annotated and validated response. """
                            self.cache.set (key, response)
                            
                        """ Add processed edges to the overall result. """
                        result += [ edge for edge, node in response ]
                        logger.debug(f"{key} => {Text.short(response)}")

                        """ Response edges go in the collector to become input for the next operation. """
                        frame.collector += response
                    except Exception as e:
                        traceback.print_exc()
                        logger.error("Error invoking> {key}")
        logger.debug (f"returning {len(result)} values.")
        return result

def execute_query (args, outputs, rosetta):
    """ Query rosetta. """
    blackboard = rosetta.construct_knowledge_graph(**args)
    """ Lower case all output values. """
    expect = list(map (lambda v : v.lower (), outputs['nodes']))
    """ Make a list of result ids. """
    ids = [ e.target_node.identifier.lower() for e in blackboard ]
    logger.debug (f"Received {len(ids)} nodes.")
    logger.debug (f"Expected {len(expect)} nodes.")
    logger.debug (f"  ==> ids: {ids}")
    matched = 0
    for o in expect:
        if o in ids:
            matched = matched + 1
        else:
            logger.error (f" {o} not in ids")
    assert matched == len(expect)
    return blackboard, ids
    
def test_disease_gene (rosetta):
    execute_query (**{
        "args" : {
            "inputs" : {
                "disease" : [
                    "DOID:2841"
                ]
            },            
            "query" :
            """MATCH (a:disease),(b:gene), p = allShortestPaths((a)-[*]->(b))
            WHERE NONE (r IN relationships(p) WHERE type(r) = 'UNKNOWN' OR r.op is null) 
            RETURN p"""
        },
        "outputs" : {
            "nodes" : ['ncbigene:191585', 'ncbigene:2289', 'ncbigene:4057', 'ncbigene:1442', 'ncbigene:4843', 'ncbigene:165829', 'ncbigene:5739', 'ncbigene:79034', 'ncbigene:7031', 'ncbigene:1048', 'ncbigene:80206', 'ncbigene:6541', 'ncbigene:340547', 'ncbigene:55600', 'ncbigene:55076', 'ncbigene:9173', 'ncbigene:115362', 'ncbigene:85413', 'ncbigene:948', 'ncbigene:56521', 'ncbigene:2043', 'ncbigene:133308', 'ncbigene:1359', 'ncbigene:1475', 'ncbigene:1469', 'ncbigene:1803', 'ncbigene:6402', 'ncbigene:11254', 'ncbigene:5625', 'ncbigene:29126', 'ncbigene:137835', 'ncbigene:5744', 'ncbigene:10964', 'ncbigene:10085', 'ncbigene:6783', 'ncbigene:6318', 'ncbigene:7903', 'ncbigene:55107', 'ncbigene:3081', 'ncbigene:60437', 'ncbigene:1178', 'ncbigene:59340', 'ncbigene:7033', 'ncbigene:760', 'ncbigene:1470', 'ncbigene:3371', 'ncbigene:10631', 'ncbigene:6528', 'ncbigene:400823', 'ncbigene:117157', 'ncbigene:405753', 'ncbigene:154064', 'ncbigene:202333', 'ncbigene:150', 'ncbigene:1179', 'ncbigene:84830', 'ncbigene:2015', 'ncbigene:25803', 'ncbigene:5055', 'ncbigene:883', 'ncbigene:3171', 'ncbigene:202309', 'ncbigene:4915', 'ncbigene:51301', 'ncbigene:131450', 'ncbigene:26998', 'ncbigene:10344', 'ncbigene:6280', 'ncbigene:90102', 'ncbigene:2206', 'ncbigene:960', 'ncbigene:246', 'ncbigene:9982', 'ncbigene:9245', 'ncbigene:3550', 'ncbigene:50506', 'ncbigene:27306', 'ncbigene:8875']
        }
    },
    rosetta=rosetta)
    
def test_drug_pathway (rosetta):
    execute_query (**{
        "args" : {
            "inputs" : {
                "drug" : [
                    "MESH:D000068877",
                    "DRUGBANK:DB00619"
                ],
            },
            "query" :
            """MATCH (a:drug),(b:pathway), p = allShortestPaths((a)-[*]->(b)) 
            WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN' OR r.op is null) 
            RETURN p"""
        },
        "outputs" : {
            "nodes" : [
                "REACT:R-HSA-6799990"
            ]
        }
    },
    rosetta=rosetta)

def run_test_suite ():
    test_disease_gene ()
    test_drug_pathway()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rosetta.')
    parser.add_argument('--debug', help="Debug", action="store_true", default=False)
    parser.add_argument('--delete-type-graph',
                        help='Delete the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('--initialize-type-graph',
                        help='Build the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('-d', '--redis-host', help='Redis server hostname.', default=None)
    parser.add_argument('-s', '--redis-port', help='Redis server port.', default=None)
    parser.add_argument('-t', '--test', help='Redis server port.', action="store_true", default=False)
    args = parser.parse_args()

    if args.debug:
        logger.info ("setting debug log level")
        #logger.setLevel (logging.DEBUG)
        logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)

    if args.initialize_type_graph or args.delete_type_graph:
        rosetta = Rosetta(init_db=args.initialize_type_graph,
                          delete_type_graph=args.delete_type_graph,
                          debug=args.debug)
    if args.test:
        run_test_suite ()
