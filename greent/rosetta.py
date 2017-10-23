import json
import logging
import logging
import networkx as nx
import networkx.algorithms as nxa
import operator
import os
import sys
import traceback
import unittest
import yaml
from greent.async import AsyncUtil
from greent.util import LoggingUtil
from networkx.exception import NetworkXNoPath
from pprint import pformat
from reasoner.graph_components import KNode,KEdge,elements_to_json

logger = LoggingUtil.init_logging (__file__, logging.DEBUG)

class Translation (object):
    def __init__(self, obj, type_a=None, type_b=None, description="", then=None):
        self.obj = obj
        self.type_a = type_a
        self.type_b = type_b
        self.desc = description
        self.then = []
        self.response = None
    def __repr__(self):
        return "Translation(obj: {0} type_a: {1} type_b: {2} desc: {3} then: {4} response: {5})".format (
            self.obj, self.type_a, self.type_b, self.desc, "",
            pformat (self.response [: min(len(self.response), 2)] if self.response else ""))

class Rosetta:
    def __init__(self, greentConf="greent.conf", config_file=os.path.join (os.path.dirname (__file__), "rosetta.yml"), override={}):
        """ Load the config file and set up a DiGraph representing the types we know about and how to transition between them. """
        from greent.core import GreenT
        self.core = GreenT (config=greentConf, override=override)
        self.g = nx.DiGraph ()

        logger.debug ("Loading Rosetta config file: {0}".format (config_file))
        with open (config_file, 'r') as stream:
            self.config = yaml.load (stream)
            
        # Prime the vocabulary
        logger.debug ("  -- Initializing Rosetta vocabulary")
        self.vocab = self.config["@vocab"]
        for k in self.vocab:
            self.g.add_node (self.vocab[k])

        # Store the concept dictionary
        logger.debug ("  -- Initializing Rosetta concept dictionary")
        self.concepts = self.config["@concepts"]
        # Build a curie map. import cmungall's uber context.
        self.curie = self.config["@curie"]
        with open(os.path.join (os.path.dirname (__file__), "jsonld", "uber_context.jsonld"), "r") as stream:
            uber = json.loads (stream.read ())
            context = uber['@context']
            for k in context:
                self.curie[k] = context[k]
                self.vocab[k] = context[k]

        # Build the transition graph.
        logger.debug ("  -- Initializing Rosetta transitions")
        transitions = self.config["@transitions"]
        for L in transitions:
            for R in transitions[L]:
                self.add_edge (L, R, data=transitions[L][R])
                self.add_edge (self.vocab[L], self.vocab[R], data=transitions[L][R])
                self.add_edge (self.vocab[L], R, data=transitions[L][R])

    def add_edge (self, L, R, data):
        #logger.debug ("  +edge: {0} {1} {2}".format (L, R, data))
        self.g.add_edge (L, R, data=data)
        
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
        the_type = self.guess_type (thing.identifier) if thing and thing.identifier else None
        return [ the_type ] if the_type else self.concepts[object_type] if object_type in self.concepts else [ object_type ] #None

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

    def get_transitions (self, source, target):
        """ Consulting the type graph, ask for paths between the source and destination types. 
        Turn those paths into trasitions - operators to call to effect the speicifed conversions from source->target."""
        transitions = []
        try:
            paths = nxa.all_shortest_paths (self.g, source=source, target=target)
            count = 0
            for path in paths:
                count += 1
                logger.debug ("  path: {0}".format (path))
                steps = list(zip(path, path[1:]))
                logger.debug ("  steps: {}".format (steps))
                for step in steps:
                    edges = self.g.edges (step, data=True)
                    for e in edges:
                        if step[1] == e[1]: # something feels hokey about this.
                            logger.debug ("    trans: {0}".format (e))
                            transition = e[2]['data']['op']
                            transitions.append (transition)
            if count == 0:
                logger.debug ("No paths found between {0} and {1}".format (source, target))
        except NetworkXNoPath:
            pass
        except KeyError:
            pass
        return transitions
    
    def process_translations (self, subject_node, object_type):
        """ Given a subject node and object type, do whatever is involved in walking the graph from the subject
        to the object type. Figure out types. Get transitions. Execute the transitions. Aggregate and return results."""
        result = [ ]
        translations = self.get_translations (subject_node, object_type)
        for translation in translations:
            data = self.translate (thing=translation.obj,
                                   source=translation.type_a,
                                   target=translation.type_b)
            result += data if isinstance(data,list) else []
        return result
    
    def translate (self, thing, source, target):
        """ Given a set of translation requests (go from A->B), get transitions (actual operators to execute A->B).
        Prime the response stack with the input node and a null edge - to be discarded before returning.
        Get transitions based on guessed types.
        For each transition, execute it against each node in the list at the top of the stack.
        This builds a new list which we add to the top of the stack. """
        if not thing:
            return None
        stack = [ [ ( None, thing ) ] ]
        transitions = self.get_transitions (
            source=self.guess_type (thing, source),
            target=self.guess_type (None, target))
        for transition in transitions:
            try:
                if len(stack) == 0:
                    break
                data_op = operator.attrgetter(transition)(self.core)
                top = stack[-1:][0] # stack[-1] is a slice of the stack list. stack[-1][0] is an alement of the stack list.
                new_top = []
                cycle = 0
                for i in top:
                    cycle += 1
                    node = i[1]
                    if cycle < 3:
                        logger.debug ("            invoke(cyc:{0}): {1}({2}) => ".format (cycle, transition, node)),
                    result = [ m for m in data_op (node) ]
                    new_top += result
                    if cycle < 3:
                        r_text = str(result)
                        logger.debug ("              response>: {0}".format (r_text[:min(len(r_text),110)] + ('...' if len(r_text)>110 else '')))
                stack.append (new_top)
            except:
                traceback.print_exc ()
        response = [ pair for level in stack for pair in level if isinstance(pair,tuple) and isinstance(pair[0], KEdge) ]
        text = str(response)
        text = (text[:100] + '...') if len(text) > 100 else text
        return response
    
if __name__ == "__main__":
    translator = Rosetta (override={ 'async' : True })
    translator.process_translations (KNode("NAME:diabetes", "D"), "A") #"http://identifier.org/hetio/cellcomponent")
#    translator.process_translations (KNode("NAME:Asthma", "D"), "mesh_disease_id")
#    translator.process_translations (KNode("NAME:diabetes", "D"), "C") #"http://identifier.org/hetio/cellcomponent")
#    translator.process_translations (KNode("DOID:2841", "D"), "G")
#    translator.process_translations (KNode("UNIPROT:AKR1B1", "G"), "P")
