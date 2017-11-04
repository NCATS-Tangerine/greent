import json
import logging
import operator
import os
import sys
import traceback
import unittest
import yaml
from enum import Enum
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import Text
from greent.util import DataStructure
from greent.service import Service
from greent.service import ServiceContext
from pprint import pformat, pprint
from neo4jrestclient.client import GraphDatabase,Relationship,Node

logger = LoggingUtil.init_logging (__file__)#, level=logging.DEBUG)

class TypeGraph(Service):
    """ A graph of 
           * nomenclature systems
           * conceptual domains in which they participate and
           * executable transitions translating from one nomenclature system to another
        Transitions specify semantics of operations used.
        Each nomenclature system is referred to as a Type and recieves a label in the graph accordingly.
        Each concept is created as a node. Each type node with an associated concept 
           * Receives a label for the connected concept
           * Is the source of an is_a link connecting to the concept node.
    """
    def __init__(self, service_context):
        super (TypeGraph, self).__init__("rosetta-graph", service_context)
        url = "{0}/db/data/".format (self.url)        
        self.db = GraphDatabase(url)
        self.types = self.db.labels.create("Type")
        self.concepts = {}
        self.type_to_concept = {}
        self.concept_metadata = None
    def set_concept_metadata (self, concept_metadata):
        logger.debug ("-- Initializing bio types.")
        self.concept_metadata = concept_metadata
        for concept, instances in self.concept_metadata.items ():
            self.concepts[concept] = self.db.labels.create (concept)
            for instance in instances:
                logger.debug ("Registering conept {} for instance {}".format (
                    concept, instance))
                self.type_to_concept [instance] = concept
    def get_concept (self, item):
        return self.type_to_concept.get (item)
    def get_relationships (self, a, b):
        q = "MATCH (a:Type { name:'%s' })-[r]-(b:Type { name:'%s' }) return r".format (a, b)
        return self.db.query(q, returns=(client.Node, str, client.Relationship), data_contents=True)
    def find_or_create (self, name, iri=None):
        n = self.types.get (name=name)
        if len(n) == 1:
            n = n[0]
        elif len(n) > 1:
            raise ValueError ("Unexpected non-unique node: {}".format (name))
        else:
            n = self.types.create (name=name, iri=iri)
            concept = self.get_concept (name)
            if concept:
                logger.debug ("   adding node {} to concept {}".format (name, concept))
                self.concepts[concept].add (n)
                concept_node = self._find_or_create_concept (concept)
                n.relationships.create ("is_a", concept_node)
        return n
    def set_node_property (self, node_name, key, value):
        node = self.db.node (name=node_name)
        node.set (key, value)        
    def add_edge (self, a, b, rel_name, predicate, op):
        a_node = self.find_or_create (a)
        b_node = self.find_or_create (b)
        a_rels = a_node.relationships.outgoing(rel_name, b_node)
        exists = False
        for rel in a_rels:
            if rel.properties.get ('op',None) == op:
                exists = True
        if not exists:
            enabled = predicate != "UNKNOWN"
            synonym = predicate == "SYNONYM"
            a_node.relationships.create (rel_name, b_node, predicate=predicate, op=op,
                                         enabled=enabled, synonym=synonym)
    def get_shortest_paths (self, a, b):
        return self.db.query (
            "MATCH (a:Type { name: '%s' }),(b:Type { name : '%s' }), p = allShortestPaths((a)-[*]-(b)) RETURN p" % (a,b),
            data_contents=True)
    def get (self, url):
        return requests.get(url).json ()
    def get_transitions0 (self, query):
        program = []
        result = self.db.query (query, data_contents=True)
        for row in result.rows[0]:
            print ("row: {}".format (json.dumps (row, indent=2)))
            node_type = None
            for col in row:
                #print ("col-> {} {}".format (col, type(col)))
                if isinstance (col, str):
                    logger.debug ("graph transition builder: noting result type: {0}".format (col))
                    node_type = col.split('>')[0] if '>' in col else col
                elif isinstance(col, dict):
                    if 'name' in col:
                        logger.debug ("graph transition builder: noting result type: {0}".format (col))
                        node_type = col['name']
                    if 'op' in col:
                        logger.debug ("graph transition builder: is dict.")
                        op = col['op']
                        logger.debug ("  --and has op")
                        is_new = True
                        for level in program:
                            if level['node_type'] == node_type:
                                logger.debug ("  -- adding op {0} to level".format (op))
                                level['ops'].append (op)
                                is_new = False
                        if is_new:
                            logger.debug ("  -- creating program component for node type {0}".format (node_type))
                            program.append ({
                                'node_type' : node_type,
                                #'ouput_type
                                'ops'       : [ op ],
                                'collector' : [] 
                            })
        return program

    def get_transitions (self, query):
        programs = []
        result = self.db.query (query, data_contents=True)
        for row_set in result.rows:
            program = []
            for row in row_set:
                node_type = None
                for col in row:
                    if isinstance (col, str):
                        logger.debug ("graph transition builder: noting result type: {0}".format (col))
                        node_type = col.split('>')[0] if '>' in col else col
                    elif isinstance(col, dict):
                        if 'name' in col:
                            logger.debug ("graph transition builder: noting result type: {0}".format (col))
                            node_type = col['name']
                        if 'op' in col:
                            logger.debug ("graph transition builder: is dict.")
                            op = col['op']
                            predicate = col['predicate']
                            logger.debug ("  --and has op")
                            is_new = True
                            for level in program:
                                if level['node_type'] == node_type:
                                    logger.debug ("  -- adding op {0} to level".format (op))
                                    level['ops'].append ({
                                        'link' : predicate,
                                        'op'   : op
                                    })
                                    is_new = False
                            if is_new:
                                logger.debug ("  -- creating component for node type {0}".format (node_type))
                                program.append ({
                                    'node_type' : node_type,
                                    'ops'       : [
                                        {
                                            'link' : predicate,
                                            'op'   : op
                                        }
                                    ],
                                    'collector' : [] 
                                })
            programs.append (program)
        return programs
    
    def _find_or_create_concept (self, concept):
        '''
        style = {
            'Anatomy'            : { 'color' : 'pink' },
            'BiologicalProcess'  : { 'color' : 'lightgray' },
            'CellularComponent'  : { 'color' : 'green' },
            'Disease'            : { 'color' : 'red' },
            'Gene'               : { 'color' : 'yellow' },
            'GeneticCondition'   : { 'color' : 'gray' },
            'MolecularFunction'  : { 'color' : 'darkgray' },
            'Name'               : { 'color' : 'darkblue' },
            'Pathway'            : { 'color' : 'lightgreen' },
            'Phenotype'          : { 'color' : 'lightgreen' },
            'Substance'          : { 'color' : 'purple' }
        }
        '''
        concept_node = self.concepts[concept].get (name=concept)
        if len(concept_node) == 1:
            logger.debug ("-- Loaded existing concept: {0}".format (concept))
            concept_node = concept_node[0]
        elif len(concept_node) > 1:
            raise ValueError ("Unexpected non-unique concept node: {}".format (concept))
        else:
            logger.debug ("-- Creating concept {0}".format (concept))
#            color = style.get (concept, {}).get ('color', '')
#            concept_node = self.concepts[concept].create (name=concept, color=color)
            concept_node = self.concepts[concept].create (name=concept)
        return concept_node
