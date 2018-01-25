import logging
import traceback

from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.exceptions import TransactionException

from greent.service import Service
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)


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

    def __init__(self, service_context):
        """ Construct a type graph, registering labels for concepts and types. """
        super(TypeGraph, self).__init__("rosetta-graph", service_context)
        self.url = "{0}/db/data/".format(self.url)
        self.initialize_connection()
        self.concepts = {}
        self.type_to_concept = {}
        self.concept_metadata = None

    def initialize_connection(self):
        logger.debug("Creating type labels")
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

    def set_concept_metadata(self, concept_metadata):
        """ Set the concept metadata. """
        logger.debug("-- Initializing bio types.")
        self.concept_metadata = concept_metadata
        for concept, instances in self.concept_metadata.items():
            self.concepts[concept] = self.db.labels.create(concept)
            for instance in instances:
                logger.debug("Registering concept {} for instance {}".format(
                    concept, instance))
                self.type_to_concept[instance] = concept

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
                logger.debug("   adding node {} to concept {}".format(name, concept))
                self.concepts[concept].add(n)
                concept_node = self._find_or_create_concept(concept)
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
                a_concept = self.type_to_concept.get(a)
                b_concept = self.type_to_concept.get(b)
                a_concept_node = self._find_or_create_concept(a_concept)
                b_concept_node = self._find_or_create_concept(b_concept)
                CONCEPT_RELATION_NAME = "translation"
                concept_rels = a_concept_node.relationships.outgoing(CONCEPT_RELATION_NAME)
                if b_concept_node not in [rel.end for rel in concept_rels]:
                    a_concept_node.relationships.create(CONCEPT_RELATION_NAME, b_concept_node)

    def _find_or_create_concept(self, concept):
        """ Find or create a concept object which will be linked to member type object. """
        concept_node = self.concept_label.get(name=concept)
        if len(concept_node) == 1:
            logger.debug("-- Loaded existing concept: {0}".format(concept))
            concept_node = concept_node[0]
        elif len(concept_node) > 1:
            raise ValueError("Unexpected non-unique concept node: {}".format(concept))
        else:
            logger.debug("-- Creating concept {0}".format(concept))
            concept_node = self.concept_label.create(name=concept)
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
        if result.rows is None:
            return []
        for row_set in result.rows:
            program = []
            for row in row_set:
                # logger.debug (json.dumps (row, indent=2))
                node_type = None
                for col in row:
                    if isinstance(col, str):
                        node_type = col.split('>')[0] if '>' in col else col
                    elif isinstance(col, dict):
                        if 'name' in col:
                            # logger.debug ("  --result type: {0}".format (col))
                            node_type = col['name']
                        elif 'op' in col:
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
                            if is_new:
                                program.append({
                                    'node_type': node_type,
                                    'ops': [
                                        {
                                            'link': predicate,
                                            'op': op
                                        }
                                    ],
                                    'collector': []
                                })
            programs.append(program)
        return programs
