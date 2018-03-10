import logging
import traceback

from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.exceptions import TransactionException
from neo4jrestclient.exceptions import NotFoundError
 
from greent.concept import Concept
from greent.concept import ConceptModel
from greent.service import Service
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging(__file__, level=logging.INFO)

class TypeGraph(Service):
    """ A graph of 
           * nomenclature systems
           * conceptual domains in which they participate and
           * executable transitions translating from one nomenclature system to another
        Transitions specify the semantics by which operations convert between nomanclature systems.
        Each nomenclature system is referred to as a Type and recieves a label in the graph.
        Each concept is created as a node. Each type node with an associated concept 
           * Receives a label for the connected concept
           * Is the source of an is_a link connecting to the concept node.
        This enables queries between concept spaces to return alternative paths of operations
    """

    def __init__(self, service_context, concept_model_name="biolink-model", debug=False):
        """ Construct a type graph, registering labels for concepts and types. """
        if debug:
            logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)
        super(TypeGraph, self).__init__("rosetta-graph", service_context)
        self.url = "{0}/db/data/".format(self.url)
        self.initialize_connection()
        self.concepts = {}
        self.type_to_concept = {}
        self.concept_model_name = concept_model_name
        self.set_concept_model ()

    def initialize_connection(self):
        logger.debug("  -+ Connecting to graph database.")
        self.db = GraphDatabase(self.url)
        self.types = self.db.labels.create("Type")
        self.concept_label = self.db.labels.create("Concept")

    def delete_all(self):
        """ Delete the type-graph only.  Leave result graphs alone. """
        try:
            with self.db.transaction(for_query=True, commit=True, using_globals=False) as transaction:
                self.db.query("MATCH (n:Concept) DETACH DELETE n")
                self.db.query("MATCH (n:Type) DETACH DELETE n")
            self.initialize_connection()
        except Exception as e:
            traceback.print_exc()

    def set_concept_model(self):
        """ Set the concept metadata. """
        logger.debug("-- Initializing graph semantic concepts.")
        self.concept_model = ConceptModel (self.concept_model_name)
        for concept_name, concept in self.concept_model.items():
            if len(concept.id_prefixes) > 0:
                logger.debug("  -+ associating concept {} with prefixes {}".format(
                    concept_name, concept.id_prefixes))
            for identifier in concept.id_prefixes:
                self.type_to_concept[identifier] = concept #_name

    def get_concept_label (self, concept):
        try:
            concept_node = self._find_or_create_concept (concept)
            if concept.is_a:
                base_class = self._find_or_create_concept (concept.is_a)
                rels = concept_node.relationships.all()
                already_linked = False
                for r in rels:
                    if r.end == base_class:
                        already_linked = True
                        break
                if not already_linked:
                    concept_node.relationships.create ("is_a", base_class)
            label = self.db.labels.get (concept.name)
        except KeyError:
            self.concepts[concept.name] = self.db.labels.create (concept.name)
        return self.concepts[concept.name]
    def add_concept_labels (self, concept, node):
        try:
            label = self.get_concept_label (concept)
            label.add (node)
            '''
            if concept.is_a:
                self.add_concept_labels (concept.is_a, node)
            '''
        except KeyError:
            logger.debug ("  -- error - unable to find concept {}".format (concept.name))
                
    def find_or_create(self, name, iri=None):
        """ Find a type node, creating it if necessary. """
        n = self.types.get(name=name)
        if len(n) == 1:
            n = n[0]
        elif len(n) > 1:
            raise ValueError("Unexpected non-unique node: {}".format(name))
        else:
            n = self.types.create(name=name, iri=iri)
            concept = self.type_to_concept.get(name)
            if concept:
                logger.debug("   adding node {} to concept {}".format(name, concept.name))
                concept_node = self._find_or_create_concept (concept)
                self.add_concept_labels (concept, n)
                n.relationships.create("is_a", concept_node)
        return n

    def add_edge(self, a, b, rel_name, predicate, op):
        """ Create a transition edge between two type nodes, storing the semantic predicate
        and transition operation.  Also create an edge between the two concepts."""
        a_node = self.find_or_create(a)
        b_node = self.find_or_create(b)
        a_rels = a_node.relationships.outgoing(rel_name, b_node)
        exists = False
        for rel in a_rels:
            if rel.properties.get('op', None) == op:
                exists = True
        if not exists:
            enabled = predicate != "UNKNOWN"
            synonym = predicate == "SYNONYM"
            if enabled:
                a_node.relationships.create(rel_name, b_node, predicate=predicate, op=op,
                                            enabled=enabled, synonym=synonym)
            if enabled and not synonym:
                # Make a translation link between the concepts
                self.add_concepts_edge (a, b, predicate, op)
                '''
                a_concept = self.type_to_concept.get(a)
                b_concept = self.type_to_concept.get(b)
                a_concept_node = self._find_or_create_concept(a_concept)
                b_concept_node = self._find_or_create_concept(b_concept)
                CONCEPT_RELATION_NAME = "translation"
                concept_rels = a_concept_node.relationships.outgoing(CONCEPT_RELATION_NAME)
                found = False
                for rel in concept_rels:
                    if rel.end == b_concept_node and rel.get('op') == op:
                        found = True
                if not found:
                    a_concept_node.relationships.create(CONCEPT_RELATION_NAME, b_concept_node, predicate=predicate,
                                                        op=op, enabled=enabled)
                '''
    def add_concepts_edge (self, a, b, predicate, op):
        a_concept = self.concept_model.get (a)
        b_concept = self.concept_model.get (b)
        assert a_concept, f"Unable to find concept {a}"
        assert b_concept, f"Unable to find concept {b}"
        a_concept_node = self._find_or_create_concept(a_concept)
        b_concept_node = self._find_or_create_concept(b_concept)
        concept_rels = a_concept_node.relationships.outgoing(predicate)
        found = False
        for rel in concept_rels:
            if rel.end == b_concept_node and rel.get('op') == op:
                found = True
        if not found:
            a_concept_node.relationships.create(predicate, b_concept_node, predicate=predicate,
                                                op=op, enabled=True)
        
    def _find_or_create_concept(self, concept):
        """ Find or create a concept object which will be linked to member type object. """
        concept_node = None
        try:
            concept_node = self.concept_label.get(name=concept.name)
            if len(concept_node) == 1:
                concept_node = concept_node[0]
            elif len(concept_node) > 1:
                raise ValueError("Unexpected non-unique concept node: {}".format(concept.name))
            else:
                logger.debug("--+ add concept {0}".format(concept.name))
                concept_node = self.concept_label.create(name=concept.name)
                
                label = self.get_concept_label (concept)
                label.add (concept_node)
        except:
            print ("concept-> {}".format (concept.name))
            traceback.print_exc ()
            traceback.print_stack ()
        return concept_node

    def run_cypher_query(self,query):
        try:
            result = self.db.query(query, data_contents=True)
        except TransactionException:
            print("Error Generated by:")
            print (query)
            return None
        return result

    def get_transitions(self, query):
        """ Execute a cypher query and walk the results to build a set of transitions to execute. """
        programs = []
        result = self.db.query(query, data_contents=True)
        import json
        if result.rows is None:
            return []
        for row_set in result.rows:
            logger.debug (row_set)
            program = []
            for row in row_set:
                #logger.debug (json.dumps (row, indent=2))
                node_type = None
                if len(row) != 3:
                    logger.error("Better check on the program")
                    logger.error (json.dumps (row, indent=2))
                    exit()
                node_type = row[0]['name']
                col = row[1]
                op = col['op']
                predicate = col['predicate']
                is_new = True
                for level in program:
                    if level['node_type'] == node_type:
                        level['ops'].append({
                            'link': predicate,
                            'op': op
                        })
                        is_new = False
                next_type = row[2]['name']
                if is_new:
                    program.append({
                        'node_type': node_type,
                        'ops': [
                            {
                                'link': predicate,
                                'op': op
                            }
                        ],
                        'next_type': next_type,
                        'collector': []
                    })
            programs.append(program)
        return programs
