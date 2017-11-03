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
from enum import Enum
from greent.async import AsyncUtil
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
from reasoner.graph_components import KNode,KEdge,elements_to_json
import requests
import requests_cache
from networkx.readwrite import json_graph
from neo4jrestclient.client import GraphDatabase,Relationship,Node

requests_cache.install_cache('rosetta_cache')
logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

class Translation (object):
    """ A translation is a conceptual container for some thing, its type, and an 
    object type to convert it to. """    
    def __init__(self, obj, type_a=None, type_b=None, description="", then=None):
        self.obj = obj
        self.type_a = type_a
        self.type_b = type_b
    def __repr__(self):
        return "Translation(obj: {0} type_a: {1} type_b: {2})".format (
            self.obj, self.type_a, self.type_b)

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
                 init_db=False):
        """ Load the config file and set up a DiGraph representing the types we know 
        about and how to transition between them. """
        from greent.core import GreenT
        self.debug = False

        # Construct the GreenT core containing services.
        logger.debug ("-- Initialize GreenT core.")
        self.core = GreenT (config=greentConf, override=override)

        '''
        # Create a digraph.
        self.g = nx.DiGraph ()
        '''
        # Load the type graph schematic.
        logger.debug ("-- Loading Rosetta config file: {0}".format (config_file))
        with open (config_file, 'r') as stream:
            self.config = yaml.load (stream)
            
        # Prime the vocabulary
        logger.debug ("-- Initializing vocabulary.")
        self.curie = {} #self.config["@curie"]
        self.to_curie_map = {}
        logger.debug ("-- Initializing Rosetta vocabulary")
        self.vocab = self.config["@vocab"]
        for k in self.vocab:
            #self.g.add_node (self.vocab[k])
            self.to_curie_map[self.vocab[k]] = k

        # Store the concept dictionary
        logger.debug ("-- Initializing Rosetta concept dictionary")
        self.concepts = self.config["@concepts"]
        self.type_graph = TypeGraph (self.core.service_context)
        
        # Build a curie map. import cmungall's uber context.
        uber = Resource.get_resource_obj (os.path.join ("jsonld", "uber_context.jsonld")) #os.path.join (os.path.dirname (__file__), "jsonld", "uber_context.jsonld")
        context = uber['@context']
        self.terminate (context)
        for key, value in context.items ():
            self.curie[k] = value
            if isinstance (value, str):
                self.vocab[k] = value

        """ Create curie map. Initialize based on the Identifiers.org web API."""
        logger.debug ("-- Initializing curie map incorporating Identifiers.org vocabulary.")
        identifiers_org = Resource.get_resource_obj ('identifiers.org.json')
        for module in identifiers_org:
            curie = module['prefix'].upper ()
            url = module['url']
            self.curie[curie] = url
            self.to_curie_map[url] = curie
            self.vocab[curie] = url

        # Exit if we're not creating the data shema.
        if not init_db:
            return

        """ Create type concepts and create graph nodes for vocabulary domains. """
        self.type_graph.set_concept_metadata (self.concepts)
        for k, v in self.vocab.items ():
            if isinstance (v, str):
                self.type_graph.find_or_create (k, v)
        
        #for concept, terminologies in type_concepts.items ():
        #    self.type_graph.find_or_create_concept (concept, terminologies)
        
        # Build the transition graph.
        logger.debug ("-- Initializing Rosetta transition graph.")
        transitions = self.config["@transitions"]
        '''
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
                self.add_edge (L, R, data=transitions[L][R])
                self.add_edge (self.vocab[L], self.vocab[R], data=transitions[L][R])
                self.add_edge (self.vocab[L], R, data=transitions[L][R])
                self.add_edge (L, self.vocab[R], data=transitions[L][R])
        '''
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
            
        # Connect the Translator Registry
        logger.debug ("-- Connecting to translator registry.")
        subscriptions = self.core.translator_registry.get_subscriptions ()
        for s in subscriptions:
            t_a_iri = s[0]
            t_b_iri = s[1]
            method = s[2]
            op = "translator_registry.{0}".format (method["op"])
            if not 'link' in method or method['link'] == None:
                link='unknown' #continue
            link = link.upper ()
            #self.add_edge (t_a_iri, t_b_iri, op)
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
                
    def add_edge (self, L, R, data):
        if self.debug:
            logger.debug ("  +edge: {0} {1} {2}".format (L, R, data))
        self.g.add_edge (L, R, data=data)
        #self.type_graph.add_edge (L, R, data=data['op'])
        
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

    def get_translations (self, thing, object_type):
        """ 
        A Thing is a node with an identifier and a concept. The identifier could really be a number, an IRI, a curie, a proper noun, etc.
        A concept is a broad notion that maps to many specific real world types.
        Our job here is to do the best we can to figure out what specific type this thing is. i.e., narrower than a concept.
        1. One way to do that is by looking at curies. If it has one we know, use the associated IRI.
        2. If that doesn't work, map the concept to the known specific types associated with it,.
        3. Do this for thing a and thing b.
        4. We want to guess specifically via the curie if possible, to avoid a combinatoric explosion resulting from cross product-ing spurious types.
        """
        x_type_a = self.map_concept_types (thing, thing.node_type)
        x_type_b = self.map_concept_types (thing=None, object_type=object_type)
        logger.debug ("Mapped types: {0} : {1}".format (x_type_a, x_type_b))
        translations = [ Translation(thing, ta_i, tb_i) for ta_i in x_type_a for tb_i in x_type_b ] if x_type_a and x_type_b else []
        for t in translations:
            logger.debug ("%s", t)
        return translations

    def get_transitions0 (self, source, target):
        """ Consulting the type graph, ask for paths between the source and destination types. 
        Turn those paths into trasitions - operators to call to effect the speicifed conversions from source->target."""
        transitions = []
        if not source or not target:
            return transitions
        try:
            # Get shortest paths between source and destination.
            paths = nxa.all_shortest_paths (self.g, source=source, target=target)
            #paths = nxa.all_simple_paths (self.g, source=source, target=target)
            count = 0
            for path in paths:                
                count += 1
                logger.debug ("  path: {0}".format (path))

                # Create pairs of types to indicates steps in the path.
                steps = list(zip(path, path[1:]))
                logger.debug ("  steps: {}".format (steps))
                for step in steps:

                    # Get edges associated with this pari
                    edges = self.g.edges (step, data=True)
                    for e in edges:
                        
                        if e[0] == e[1]:
                            # For now, let's not do transitions to self.
                            continue
                        
                        if step[1] == e[1]:
                            # Collect the transition method for these types into our output.
                            logger.debug ("    trans: {0}".format (e))
                            transition = e[2]['data']['op']
                            transitions.append (( e[0], transition ))
            if count == 0:
                logger.debug ("No paths found between {0} and {1}".format (source, target))
        except NetworkXNoPath:
            #traceback.print_exc ()
            #self.debug_graph (source, target)
            pass
        except KeyError:
            pass
        return transitions

    def to_curie (self, text):
        return self.to_curie_map.get (text, None)
    
    def make_up_curie (self, text):
        """ If we got one, great. Yay for standards. If not, get creative. This is legitimate and important because we can't have
        useful automated reasoning without granular semantics. But folks make more specific sub domain names which is probably
        essential for semantics and therefore automation. Until we arrive at a better approach, lets accept this approach and make up
        a curie if the service author thought it was important to have one."""
        curie = self.to_curie (text)
        if not curie:
            pieces = text.split ('/')
            last = pieces[-1:][0]
            curie = last.upper ()
        return curie
    
    def get_transitions (self, source, target):
        return self.type_graph.get_transitions (
            self.to_curie (source),
            self.to_curie (target))
    def process_translations (self, subject_node, object_type):
        """ Given a subject node and object type, do whatever is involved in walking the graph from the subject
        to the object type. Figure out types. Get transitions. Execute the transitions. Aggregate and return results."""
        result = [ ]

        # Consult the graph and get valid translations. This is essentially the output of the cross product of
        # potential specific types derived from abstract types like 'D' or 'S'
        translations = self.get_translations (subject_node, object_type)

        # Execute the translations. Dig down to a lower level and actually execute the transitions.
        for index, translation in enumerate (translations):            
            #logger.debug ("{0} translation:".format (index))
            data = self.translate (thing=translation.obj,
                                   source=translation.type_a,
                                   target=translation.type_b)
            result += data if isinstance(data,list) else []
        return result

    def debug_graph (self, source, target):
        try:
            logger.debug ("Calculating all paths from {0} to {1}".format (source, target))
            all_paths = nxa.all_simple_paths (self.g, source=source, target=target)
            for p in all_paths:
                logger.debug ("  > from[{0}]: {1}".format (source, p[1:]))
        except Exception as e:
            logger.debug (traceback.format_exc ().split ("\n")[-2:-1])

    def get_ops (self, names):
        return operator.attrgetter(names)(self.core) if isinstance(names,str) else [ operator.attrgetter(n)(self.core) for n in names ]
        
    def translate (self, thing, source, target):
        """ Given a set of translation requests (go from A->B), get transitions (actual operators to execute A->B).
        Prime the response stack with the input node and a null edge - to be discarded before returning.
        Get transitions based on guessed types.
        For each transition, execute it against each node in the list at the top of the stack.
        This builds a new list which we add to the top of the stack. """

        # Yeah, we really need something to start with.
        if not thing:
            return None

        # Prime the stack.
        stack = [ [ ( None, thing ) ] ]

        # Consult the type graph to get a list of (potentially) useful transitions.
        transitions = self.get_transitions (
            source=self.guess_type (thing, source),
            target=self.guess_type (None, target))
        
        # Do the transitions.
        max_cycles = -1
        for transition in transitions:
            try:
                # Lookup the transition in the GreenT core.
                data_op = self.get_ops (transition[1]) #operator.attrgetter(transition[1])(self.core)

                # Top is the result of the last query invocation.
                top = stack[-1:][0] # stack[-1] is a slice of the stack list. stack[-1][0] is an alement of the stack list.
                
                # A bucket to store new results.
                new_top = []

                # Iterate over each result in the prior set.
                for cycle, i in enumerate (top):
                    node = i[1]
                    #self.log_debug ("       top: {}".format (top))
                    self.log_debug ("           > {0}({1}) in:{2} => ".format (transition[1], node, transition[0]), cycle)

                    if max_cycles > -1 and cycle > max_cycles:
                        break
                    
                    # Invoke the transition operator.
                    result = []
                    if isinstance (data_op, list):
                        for inner, op in enumerate(data_op):
                            try:
                                if max_cycles > -1 and inner > max_cycles:
                                    break
                                result += (op (node))
                            except:
                                logger.debug ("           Error invoking {0}".format (transition[1]))
                    else:
                        try:
                            result += data_op (node)
                        except:
                            logger.error ("Error invoking {0}".format (transition[1]))

                    result = [ i for op in data_op for i in op(node) ] if isinstance(data_op, list) else data_op (node)
                    new_top += result if result is not None else []

                    if result:
                        self.log_debug ("             response>: {0}".format (Text.short(result,150)), cycle)

                # Push the new result set to top of stack.
                stack.append (self.unique_by_node_id (new_top))
                #stack.append (new_top)
            except:
                traceback.print_exc ()
            
        return [ pair for level in stack for pair in level if isinstance(pair,tuple) and isinstance(pair[0], KEdge) ]
    
    def unique_by_node_id (self,seq):
        """ Make the list edge, node tuples unique with respect to nodes. If we find a reason to want duplicate nodes, change this. """
        seen = set()
        seen_add = seen.add
        return [ x for x in seq if not (x[1].identifier in seen or seen_add(x[1].identifier)) ]
    
    def log_debug (self, text, cycle=0, if_empty=False):
        if cycle < 3:
            if (text and len(text) > 0) or if_empty:
                logger.debug ("{}".format (text))

    def translate_levels (self, node, levels):
        results = []
        last = [ (None, node) ]
        max_cycles = 5
        for index, level in enumerate (levels.split ("/")):
            new_last = []
            for subindex, result in enumerate(last):
                if max_cycles > -1 and subindex > 5:
                    break
                new_last += self.process_translations (result[1], level)
            last = new_last
            results += last
        return results

    def graph (self, next_nodes, query):
        program = self.type_graph.get_transitions_x (query)
        print ("program: {}".format (program))
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
                    op = self.get_ops (operator)
                    try:
                        #logger.debug ("--Invoking op {0}({1})".format (operator, edge_node[1].identifier))
                        log_text = "  --Invoking op {0}({1})".format (operator, edge_node[1].identifier)
                        source_node = edge_node[1]
                        results = op (source_node)
                        for r in results:
                            edge = r[0]
                            if isinstance(edge,KEdge):
                                edge.source_node = source_node
                                edge.target_node = r[1]
                                linked_result.append (edge)
                        logger.debug ("{0} => {1}".format (log_text, Text.short (results)))
                        #logger.debug ("  result> {0}".format (Text.short (results)))
                        for r in results:
                            if index < len(program) - 1:
                                if not r[1].identifier.startswith (program[index+1]['node_type']):
                                    logger.debug ("Operator {0} is wired to return type: {1} but returned node with id: {2}".format (
                                        operator, program[index+1]['node_type'], r[1].identifier))
                        collector += results
                    except Exception as e:
                        traceback.print_exc()
                        logger.error ("Error invokign> {0}".format (log_text))        
        return linked_result
            
    def clinical_outcome_pathway (self, drug=None, disease=None):
        blackboard = []
        if disease:
            blackboard += self.graph (
                [ ( None, KNode('NAME.DISEASE:{0}'.format (disease), 'D') ) ],
                query=\
                """MATCH (a{name:"NAME.DISEASE"}),(b:Gene), p = allShortestPaths((a)-[*]->(b)) 
                WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN') 
                RETURN p""")
        if drug:
            blackboard += self.graph (
                [ ( None, KNode('NAME.DRUG:{0}'.format (drug), 'S') ) ],
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
    parser.add_argument('--initialize-type-graph', help='Build the graph of types and semantic transitions between them.', action="store_true", default=False)
    parser.add_argument('-d', '--disease', help='A disease to analyze.', default=None)
    parser.add_argument('-s', '--drug', help='A drug to analyze.', default=None)
    args = parser.parse_args()
    
    rosetta = Rosetta (init_db=args.initialize_type_graph)
#    blackboard = rosetta.clinical_outcome_pathway (drug=args.drug, disease=args.disease)
    blackboard = Rosetta.clinical_outcome_pathway_app (drug=args.drug, disease=args.disease)
    print (blackboard)
