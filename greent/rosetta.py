import argparse
import json
import logging
import networkx as nx
import networkx.algorithms as nxa
import operator
import os
import sys
import traceback
import unittest
import yaml
import requests
import requests_cache
from enum import Enum
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import Text
from greent.util import DataStructure
from greent.neo4j import Neo4JREST
from greent.service import Service
from greent.service import ServiceContext
from greent.graph import TypeGraph
from networkx.exception import NetworkXNoPath
from networkx.exception import NetworkXError
from pprint import pformat,pprint
from greent.graph_components import KNode,KEdge,elements_to_json
from networkx.readwrite import json_graph
from neo4jrestclient.client import GraphDatabase,Relationship,Node

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

class Rosetta:
    """ Rosetta's translates between semantic domains generically and automatically.
    Based on a configuration file, it builds a directed graph where types are nodes.
    Types can be IRIs or CURIEs. Edges are annotated with the names of operators used
    to transition between the connected types. The engine can then accept requests to 
    translate a term from one domain to another. It does this by collecting transitions
    from the graph and executing the list of transitions. """
    
    def __init__(self, greentConf="greent.conf",
                 config_file=os.path.join (os.path.dirname (__file__), "rosetta.yml"),
                 override={},
                 delete_type_graph=False,
                 init_db=False):

        """ The constructor loads the config file an prepares the type graph. If the delete_type_graph 
        flag is true, the graph is deleted entirely. If the init_db flag is true, the type_graph will
        be loaded from the config file. """
        """ Load the config file and set up a DiGraph representing the types we know 
        about and how to transition between them. """
        from greent.core import GreenT
        self.debug = False
        self.cache_path = 'rosetta_cache'

        logger.debug ("-- Initialize GreenT service core.")
        self.core = GreenT (config=greentConf, override=override)

        logger.debug ("-- Loading Rosetta graph schematic config: {0}".format (config_file))
        with open (config_file, 'r') as stream:
            self.config = yaml.load (stream)
            
        logger.debug ("-- Initializing vocabulary and curies.")
        self.curie = {}
        self.to_curie_map = {}
        self.vocab = self.config["@vocab"]
        for k in self.vocab:
            self.to_curie_map[self.vocab[k]] = k

        logger.debug ("-- Initializing Rosetta type graph")
        self.concepts = self.config["@concepts"]
        self.type_graph = TypeGraph (self.core.service_context)

        logger.debug ("-- Extending curie map with uber_context.")
        uber = Resource.get_resource_obj (os.path.join ("jsonld", "uber_context.jsonld"))
        context = uber['@context']
        self.terminate (context)
        for key, value in context.items ():
            self.curie[k] = value
            if isinstance (value, str):
                self.vocab[k] = value

        logger.debug ("-- Merge Identifiers.org vocabulary into Rosetta vocab.")
        identifiers_org = Resource.get_resource_obj ('identifiers.org.json')
        for module in identifiers_org:
            curie = module['prefix'].upper ()
            url = module['url']
            self.curie[curie] = url
            self.to_curie_map[url] = curie
            self.vocab[curie] = url

        if delete_type_graph:
            logger.debug ("--Deleting type graph")
            self.type_graph.delete_all ()
        
        if not init_db:
            return

        logger.debug ("--Initialize concept graph metadata and create type nodes.")
        self.type_graph.set_concept_metadata (self.concepts)
        for k, v in self.vocab.items ():
            if isinstance (v, str):
                self.type_graph.find_or_create (k, v)
        
        logger.debug ("-- Initializing Rosetta transition graph.")
        transitions = self.config["@transitions"]
        errors = 0
        for L in transitions:
            for R in transitions[L]:
                if not L in self.vocab:
                    errors += 1
                    self.log_debug("{0} not in vocab.".format (L))
                    continue
                if not R in self.vocab:
                    errors += 1
                    self.log_debug ("{0} not in vocab.".format (R))
                    continue
                assert L in self.vocab and R in self.vocab
                transition_dict = transitions[L][R]
                transition_obj = DataStructure.to_named_tuple ('TransitionTuple', transitions[L][R])
                if 'link' in transition_dict and 'op' in transition_dict:
                    self.type_graph.add_edge (L, R,
                                              rel_name=transition_obj.link.upper (),
                                              predicate=transition_obj.link.upper (),
                                              op=transition_obj.op)
        if errors > 0:
            logger.error ("** Encountered {0} errors. exiting.".format (errors))
            sys.exit (errors)
            
        logger.debug ("-- Connecting to translator registry to the type graph.")
        subscriptions = self.core.translator_registry.get_subscriptions ()
        for s in subscriptions:
            t_a_iri = s[0]
            t_b_iri = s[1]
            method = s[2]
            op = "translator_registry.{0}".format (method["op"])
            if not 'link' in method or method['link'] == None:
                link='unknown' #continue
            link = link.upper ()
            t_a = self.make_up_curie (self.unterminate (t_a_iri))
            t_b = self.make_up_curie (self.unterminate (t_b_iri))
            if not t_a:
                logger.debug ("Unable to find curie for {}".format (t_b))
            elif not t_b:
                logger.debug ("Unable to find curie for {}".format (t_b))
            else:
                self.type_graph.find_or_create (t_a, iri=t_a_iri)
                self.type_graph.find_or_create (t_b, iri=t_b_iri)
                if link and op:
                    self.type_graph.add_edge (t_a, t_b, rel_name=link, predicate=link, op=op)

    def terminate (self, d):
        for k, v in d.items ():
            if isinstance(v, str) and not v.endswith ("/"):
                d[k] = "{0}/".format (v)

    def unterminate (self, text):
        return text[:-1] if text.endswith ('/') else text
        
    def guess_type (self, thing, source=None):
        """ Look for a CURIE we know. If that doesn't work, try one of our locally made up vocab words. """
        if thing and not source and ':' in thing:
            curie = thing.upper ().split (':')[0]
            if curie in self.curie:
                source = self.curie[curie]
        if source and not source.startswith ("http://"):
            source = self.vocab[source] if source in self.vocab else None
        return source

    def map_concept_types (self, thing, object_type=None):
        """ Expand high level concepts into concrete types our data sources understand. """

        # Try the CURIE approach.
        the_type = self.guess_type (thing.identifier) if thing and thing.identifier else None

        # If that didn't work, get candiddate types based on the (abstract) node type.
        if thing and not the_type:
            the_type = self.concepts.get (thing.node_type, None)
            if the_type:
                # Attempt to map them down to IRIs
                the_type = [ self.vocab.get(t,t) for t in the_type ]

        # Systematize this:
        # If the concept type is disease but the curie is NAME, we don't have a DOID.
        if isinstance(the_type,str):
            # If we've ended up with just one string, make it a list for conformity of return type
            the_type = [ the_type ]

        result = the_type if the_type else self.concepts.get (object_type, [ object_type ])

        curie = Text.get_curie (thing.identifier) if thing else None
        if curie:
            result = [ self.make_up_curie (curie) ] #[ self.vocab[curie] ]
            #result = [ self.vocab[curie] ]

        return result

    def to_curie (self, text):
        return self.to_curie_map.get (text, None)
    
    def make_up_curie (self, text):
        """ If we got one, great. Yay for standards. If not, get creative. This is legitimate and
        important because we can't have useful automated reasoning without granular semantics. But
        folks make more specific sub domain names which is probably essential for semantics and
        therefore automation. Until we arrive at a better approach, lets accept this approach and make up
        a curie if the service author thought it was important to have one."""
        curie = self.to_curie (text)
        if not curie:
            pieces = text.split ('/')
            last = pieces[-1:][0]
            curie = last.upper ()
        return curie
    
    def get_ops (self, names):
        """ Dynamically locate python methods corresponding to names configured for semantic links. """
        return operator.attrgetter(names)(self.core) if isinstance(names,str) else [
            operator.attrgetter(n)(self.core) for n in names ]
    
    def log_debug (self, text, cycle=0, if_empty=False):
        if cycle < 3:
            if (text and len(text) > 0) or if_empty:
                logger.debug ("{}".format (text))
                
    def graph (self, next_nodes, query):
        """ Given a set of starting nodes and a query, execute the query to get a set of paths.
        Each path reflects a set of transitions from the starting tokens through the graph.
        Each path is then executed and the resulting links and nodes returned. """
        programs = self.type_graph.get_transitions (query)
        result = []
        for program in programs:
            result += self.graph_inner (next_nodes, program)
        return result
    
    def graph_inner (self, next_nodes, program):
        print ("program: {}".format (json.dumps (program, indent=2)))
        if not program or len(program) == 0:
            return []
        primed = [ { 'collector' : next_nodes } ] + program
        linked_result = []
        for index, level in enumerate (program):
            logger.debug ("--Executing level: {0}".format (level))
            operators = level['ops']
            collector = level['collector']
            for edge_node in primed[index]['collector']:
                for operator in operators:
                    op = self.get_ops (operator['op'])
                    try:
                        results = None
                        log_text = "  -- {0}({1})".format (operator['op'], edge_node[1].identifier)
                        source_node = edge_node[1]
                        with requests_cache.enabled("rosetta_cache"):
                            results = op (source_node)
                        for r in results:
                            edge = r[0]
                            if isinstance(edge,KEdge):
                                edge.predicate = operator['link']
                                edge.source_node = source_node
                                edge.target_node = r[1]
                                linked_result.append (edge)
                        logger.debug ("{0} => {1}".format (log_text, Text.short (results)))
                        for r in results:
                            if index < len(program) - 1:
                                if not r[1].identifier.startswith (program[index+1]['node_type']):
                                    logger.debug (
                                        "Operator {0} wired to return type: {1} returned node with id: {2}".format (
                                            operator, program[index+1]['node_type'], r[1].identifier))
                        collector += results
                    except Exception as e:
                        traceback.print_exc()
                        logger.error ("Error invoking> {0}".format (log_text))        
        return linked_result
            
    def clinical_outcome_pathway (self, drug=None, disease=None):
        blackboard = []
        from greent import node_types
        if disease:
            blackboard += self.graph (
                [ ( None, KNode('NAME.DISEASE:{0}'.format (disease), node_types.NAME_DISEASE) ) ],
                query=\
                """MATCH (a{name:"NAME.DISEASE"}),(b:GeneticCondition), p = allShortestPaths((a)-[*]->(b)) 
                WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN') 
                RETURN p""")
            blackboard += self.graph (
                [ ( None, KNode('NAME.DISEASE:{0}'.format (disease), 'D') ) ],
                query=\
                """MATCH (a{name:"NAME.DISEASE"}),(b:Gene), p = allShortestPaths((a)-[*]->(b)) 
                WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN') 
                RETURN p""")
        if drug:
            blackboard += self.graph (
                [ ( None, KNode('NAME.DRUG:{0}'.format (drug), node_types.NAME_DRUG) ) ],
                query=\
                """MATCH (a{name:"NAME.DRUG"}),(b:Pathway), p = allShortestPaths((a)-[*]->(b)) 
                WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN') 
                RETURN p""")
        return blackboard
    
    @staticmethod
    def clinical_outcome_pathway_app (drug=None, disease=None, greent_conf='greent.conf'):
        return Rosetta(greentConf=greent_conf).clinical_outcome_pathway (drug=drug, disease=disease)

    @staticmethod
    def clinical_outcome_pathway_app_from_args (args, greent_conf='greent.conf'):        
        result = []
        if isinstance(args,list) and len(args) == 2 and \
           isinstance(args[0],str) and isinstance(args[1],str):
            result = (
                args,
                Rosetta.clinical_outcome_pathway_app (
                    drug=args[0],
                    disease=args[1],
                    greent_conf=greent_conf) )
        return result
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rosetta.')
    parser.add_argument('--delete-type-graph',
                        help='Delete the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('--initialize-type-graph',
                        help='Build the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('-d', '--disease', help='A disease to analyze.', default=None)
    parser.add_argument('-s', '--drug', help='A drug to analyze.', default=None)
    args = parser.parse_args()
    
    rosetta = Rosetta (init_db=args.initialize_type_graph,
                       delete_type_graph=args.delete_type_graph)
    blackboard = Rosetta.clinical_outcome_pathway_app (drug=args.drug,
                                                       disease=args.disease)
    print (blackboard)
