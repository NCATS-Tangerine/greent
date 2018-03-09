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
                found = False
                for rel in concept_rels:
                    if rel.end == b_concept_node and rel.get('op') == op:
                        found = True
                if not found:
                    a_concept_node.relationships.create(CONCEPT_RELATION_NAME, b_concept_node, predicate=predicate,
                                                        op=op, enabled=enabled)

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

    #TODO: There's some potential issues if there are adjacent nodes of the same type (gene-gene interactions or similarities)
    def get_transitions(self, query):
        """ Execute a cypher query and walk the results to build a set of transitions to execute.
        The query should be such that it returns a path (node0-relation0-node1-relation1-node2), and
        an array of the relation start nodes.  For the path above, start nodes like (node0,node1) would
        indicate a unidirectional path, while (node0,node2) would indicate an end-based path meeting in
        the middle.
        Each node in the path can be described with an arbitrary node index.  Note that this index does not
        have to correspond to the order of calling or any structural property of the graph.  It simply points
        to a particular node in the call map.
        Returns:
            nodes: A map from a node index to the concept.
            transitions: a map from a node index to an (operation, output index) pair
        """
        result = self.db.query(query, data_contents=True)
        import json
        if result.rows is None:
            logger.debug("No rows")
            return []
        graphs=[]
        for row in result.rows:
            #Each row_set should be a (path, startnodes) pair
            if len(row) != 2:
                logger.error("Unexpected number of elements ({})in cypher query return".format(len(row)))
                logger.error (json.dumps (row, indent=2))
                raise Exception()
            nodes = {}
            transitions = {}
            path = row[0]
            start_nodes = row[1]
            #len(start_nodes) = number of transitions
            #len(path) = number of transitions + number of nodes = 2*number of transitions + 1
            if len(path) != 2 * len(start_nodes) + 1:
                logger.error ("Inconsistent length of path and startnodes")
                logger.debug (json.dumps (row, indent=2))
                raise Exception()
            for i, element in enumerate(path):
                if i % 2 == 0:
                    #node
                    nodenum = int(i / 2)
                    nodes[nodenum] = element['name']
                else:
                    #relationship
                    predicate=element['predicate']
                    op = element['op']
                    relnum = int((i-1)/2)
                    if start_nodes[relnum] == path[i-1]:
                        from_node=relnum
                        to_node = relnum+1
                    elif start_nodes[relnum] == path[i+1]:
                        from_node=relnum+1
                        to_node = relnum
                    transitions[from_node] = { 'link': predicate,
                                               'op': op,
                                               'to': to_node}
            graphs.append( (nodes, transitions) )
        return graphs
