import json
import logging
import sys
import traceback
from collections import defaultdict

from neo4jrestclient.exceptions import TransactionException

from greent.concept import Concept
from greent.concept import ConceptModel
from greent.service import Service
from greent.util import LoggingUtil
from neo4j.v1 import GraphDatabase

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
        super(TypeGraph, self).__init__("rosetta-graph", service_context)
        if debug:
            logger.setLevel(logging.DEBUG)
        self.initialize_connection()
        self.type_to_concept = {}
        self.edges = defaultdict(list)
        self.concept_model_name = concept_model_name
        self.set_concept_model()
        self.TYPE = "Type"
        self.CONCEPT = "Concept"

    def initialize_connection(self):
        """ Connect to the database. """
        config = self.get_config()
        username = config.get("username")
        password = config.get("password")
        logger.debug(f"  -+ Connecting to graph database: {self.url}")
        self.driver = GraphDatabase.driver(self.url)
        self.db = GraphDB(self.driver.session())

    def delete_all(self):
        """ Delete the type-graph only.  Leave result graphs alone. """
        try:
            self.db.exec("MATCH (n:Concept) DETACH DELETE n")
            self.db.exec("MATCH (n:Type) DETACH DELETE n")
            self.initialize_connection()
        except Exception as e:
            traceback.print_exc()

    def set_concept_model(self):
        """ Build the concept model. """
        logger.debug("-- Initializing graph semantic concepts.")
        self.concept_model = ConceptModel(self.concept_model_name)
        for concept_name, concept in self.concept_model.items():
            if len(concept.id_prefixes) > 0:
                logger.debug("  -+ concept {} <= {}".format(
                    concept_name, concept.id_prefixes))
            for identifier in concept.id_prefixes:
                self.type_to_concept[identifier] = concept

    def build_concept(self, concept):
        """ Build a concept and its semantic backstory including is_a hierarcy. """
        if concept:
            self._find_or_create_concept(concept)
            if concept.is_a:
                """ If it has an ancestor, create a node for the ancestor and link the nodes. """
                base_class = self._find_or_create_concept(concept.is_a)
                self.db.create_relationship(
                    name_a=concept.name, type_a=self.CONCEPT,
                    properties={
                        "name": "is_a"
                    },
                    name_b=concept.is_a.name, type_b=self.CONCEPT)
            """ Recurse. """
            self.build_concept(concept.is_a)

    def find_or_create(self, name, iri=None):
        """ Find a type node, creating it if necessary. Link it to a concept. """
        properties = {"name": name, "iri": iri}
        result = self.db.get_node(properties, self.TYPE)
        n = result.peek()
        if not n:
            n = self.db.create_type(properties)
            concept = self.type_to_concept.get(name)
            if concept:
                logger.debug(f"   adding node {name} to concept {concept.name}")
                concept_node = self._find_or_create_concept(concept)
                self.build_concept(concept)
                self.db.add_label(properties={"name": name},
                                  node_type=self.TYPE,
                                  label=concept.name)
                self.db.create_relationship(
                    name_a=concept.name, type_a=self.CONCEPT,
                    properties={
                        "name": "is_a"
                    },
                    name_b=name, type_b=self.TYPE)
        return n

    def add_concepts_edge(self, a, b, predicate, op):
        """ Add an edge between two concepts. Include the operation to call to effect the transition. """
        a_concept = self.concept_model.get(a)
        b_concept = self.concept_model.get(b)
        assert a_concept, f"Unable to find concept {a}"
        assert b_concept, f"Unable to find concept {b}"
        a_concept_node = self._find_or_create_concept(a_concept)
        b_concept_node = self._find_or_create_concept(b_concept)
        self.db.create_relationship(name_a=a_concept.name, type_a=self.CONCEPT,
                                    properties={
                                        "name": predicate,
                                        "predicate": predicate,
                                        "op": op,
                                        "enabled": True
                                    },
                                    name_b=b_concept.name, type_b=self.CONCEPT)
        edge = {'source': a_concept.name, 'target': b_concept.name, 'predicate': predicate, 'op': op}
        self.edges[a_concept.name].append(edge)
        self.edges[b_concept.name].append(edge)
        return edge

    def _find_or_create_concept(self, concept):
        """ Find or create a concept object which will be linked to member type object. """
        concept_node = None
        try:
            properties = {"name": concept.name}
            result = self.db.get_node(properties, node_type=self.CONCEPT)
            concept_node = result.peek()
            if not concept_node:
                concept_node = self.db.create_node(properties, node_type=self.CONCEPT)
                self.db.add_label(properties, node_type=self.CONCEPT, label=concept.name)
        except:
            print("concept-> {}".format(concept.name))
            traceback.print_exc()
            traceback.print_stack()
        return concept_node

    def cast_edges(self, type_check_functions):
        """With a built type-graph, push edges up and down the type hierarchy (concept_map)"""
        #This approach generates a lot of edges if we let it.  And that might be the right answer
        #But for now, let's try to keep it in check
        #This is one way to do it, but we could swap it with something more complex
        usable_concepts = self.get_concepts_with_edges()
        children,push_ups = self._push_up(type_check_functions,usable_concepts)
        self._pull_down(children, type_check_functions, push_ups)

    def _push_up(self, type_check_functions, usable_concepts):
        this_level = self.concept_model.get_leaves()
        children = defaultdict(list)
        push_ups = defaultdict(list)
        while len(this_level) > 0:
            next_level = set()
            for concept in this_level:
                parent = concept.is_a
                if parent is None:
                    continue
                children[parent].append(concept)
                next_level.add(parent)
                if parent.name not in usable_concepts:
                    continue
                # push up functions that are taking or returning the child concept
                for edge in self.edges[concept.name]:
                    op = edge['op']
                    if not op.startswith('caster.'):
                        op = self.create_caster_op(op)
                    if edge['target'] == concept.name:
                        # returning child concept
                        newop = self.wrap_op('upcast', op, parent.name)
                        newedge = self.add_concepts_edge(edge['source'], parent.name, edge['predicate'], newop)
                    elif edge['source'] == concept.name:
                        # taking child concept
                        # if we have a way to filter inputs we can use it, but if not, we can just call
                        # the function and see if it works...
                        if concept.name in type_check_functions:
                            newop = self.wrap_op('input_filter', op, concept.name, type_check_functions[concept.name])
                        else:
                            newop = self.wrap_op('input_filter', op, concept.name)
                        newedge = self.add_concepts_edge(parent.name, edge['target'], edge['predicate'], newop)
                    else:
                        print(edge['source'])
                        print(concept.name)
                        exit()
                    push_ups[str(newedge)].append(concept.name)
            this_level = next_level
        return children,push_ups

    def _pull_down(self, children_dict, type_check_functions, push_ups):
        this_level = self.concept_model.get_roots()
        while len(this_level) > 0:
            next_level = set()
            for concept in this_level:
                children = children_dict[concept]
                next_level.update(children)
                for edge in self.edges[concept.name]:
                    op = edge['op']
                    if not op.startswith('caster.'):
                        op = self.create_caster_op(op)
                    for child in children:
                        if child.name in push_ups[str(edge)]:
                            #Pushed this edge up, now trying to push it right back down
                            continue
                        if edge['target'] == concept.name:
                            # returning parent
                            # Can only do this if I know how to filter.
                            try:
                                newop = self.wrap_op('output_filter', op, child.name, type_check_functions[child.name])
                                self.add_concepts_edge(edge['source'], child.name, edge['predicate'], newop)
                            except KeyError:
                                pass
                        elif edge['source'] == concept.name:
                            # taking parent, nothing else really required
                            edge = self.add_concepts_edge(child.name, edge['target'], edge['predicate'], edge['op'])
            this_level = next_level

    def wrap_op(self, func, op, arg1, arg2=None):
        p = op.split('.')
        args = [p[1], arg1]
        if arg2 is not None:
            args.append('~'.join(arg2.split('.')))
        argstring = ','.join(args)
        wrapped = '.'.join([p[0], f"{func}({argstring})"])
        return wrapped

    def create_caster_op(self, oldop):
        return 'caster.{}'.format('~'.join(oldop.split('.')))

    def run_cypher_query(self, query):
        """ Execute a cypher query and return the result set. """
        result = None
        try:
            result = self.db.query(query, data_contents=True)
        except TransactionException:
            print("Error Generated by:")
            print(query)
            result = None
        return result

    # TODO: There's some potential issues if there are adjacent nodes of the same type (gene-gene interactions or similarities)
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
        graphs = []
        result = self.db.query(query)
        for row in result:
            nodes = {}
            transitions = {}
            path = row[0]
            node_id = {node.id: i for i, node in enumerate(path.nodes)}
            node_map = {node.id: node.properties['name'] for i, node in enumerate(path.nodes)}
            for i, element in enumerate(path):
                logger.debug(f"relationship {i}> {element}")
                from_node = node_id[element.start]
                to_node = node_id[element.end]
                nodes[from_node] = node_map[element.start]
                nodes[to_node] = node_map[element.end]
                transitions[from_node] = {
                    'link': element.properties['predicate'],
                    'op': element.properties['op'],
                    'to': to_node
                }
            graphs.append((nodes, transitions))
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"{json.dumps(graphs, indent=2)}")
        return graphs

    def get_knowledge_map_programs(self, query):
        """ Execute a cypher query and walk the results to build a set of transitions to execute.
        The query should be such that it returns a path (node0-relation0-node1-relation1-node2), and
        an array of the relation start nodes. 

        This algorithm focuses on linear paths.

        Returns:
            a list of list of Frame.
        """

        """ A list of possible executable pathways enacting the input query. """
        programs = []

        """ Query the database for paths. """
        result = self.db.query(query)
        for row in result:
            logger.debug(f"row> {row} {type(row)}")
            path = row[0]
            """ One path corresponds to one program, or stack of frames. """
            program = defaultdict(Frame)
            node_map = {node.id: node.properties for i, node in enumerate(path.nodes)}
            for i, relationship in enumerate(path):
                start_node_name = node_map[relationship.start]['name']
                logger.debug(f"  -+ adding frame {start_node_name}")
                frame = program[start_node_name]
                frame.name = start_node_name
                print(f" props: {relationship.properties}")
                if 'op' in relationship.properties:
                    frame.add_operator(op=relationship.properties['op'],
                                       predicate=relationship.properties['op'])
            programs.append(list(program.values()))
            for p in programs:
                print(f"  list {p}")
        return programs

    def get_concepts_with_edges(self):
        return list( self.edges.keys() )


class Operator:
    """ Abstraction of a method to call to effect a transition between two graph nodes. """

    def __init__(self, op=None, predicate=None):
        self.op = op
        self.predicate = predicate

    def __repr__(self):
        return f"Operator(op={self.op},pred={self.predicate})"

    def __str__(self):
        return self.__repr__()


class Frame:
    """ A frame represents a set of operations to transition from one concept type to another.
    Frames may be stacked in a program. """

    def __init__(self, name=None, ops=[], collector=[]):
        self.name = name
        self.ops = defaultdict(Operator)
        self.collector = collector

    def add_operator(self, op, predicate):
        operator = self.ops[op]
        operator.op = op
        operator.predicate = predicate

    def __repr__(self):
        ops = []
        for k, op in self.ops.items():
            ops.append(str(op))
        ops = ",".join(ops)
        return f"Frame(name={self.name},ops=[{ops}])"


class GraphDB:
    """ Encapsulate graph database operations to some extent. """

    def __init__(self, session):
        self.session = session

    def exec(self, command):
        """ Execute a cypher command returning the result. """
        return self.session.run(command)

    def query(self, query):
        """ Synonym for exec for read only contexts. """
        return self.exec(query)

    def get_node(self, properties, node_type=None):
        """ Get a ndoe given a set of properties and a node type to match on. """
        ntype = f":{node_type}" if node_type else ""
        properties = ",".join([f""" {k} : "{v}" """ for k, v in properties.items()])
        return self.exec(f"""MATCH (n{ntype} {{ {properties} }}) RETURN n""")

    def create_concept(self, properties):
        """ Shortcut to create a concept node. """
        self.create_node(properties, "Concept")

    def create_type(self, properties):
        """ Shortcut to create a type node. """
        self.create_node(properties, "Type")

    def create_node(self, properties, node_type=None):
        """ Create a generic node given a set of properties and a node type. """
        ntype = f":{node_type}" if node_type else ""
        properties = ",".join([f""" {k} : "{v}" """ for k, v in properties.items()])
        return self.exec(f"""CREATE (n{ntype} {{ {properties} }}) RETURN n""")

    def add_label(self, properties, node_type, label):
        """ Add a label to a node, given properties, a type, and the label to add. """
        ntype = f":{node_type}" if node_type else ""
        properties = ",".join([f""" {k} : "{v}" """ for k, v in properties.items()])
        return self.exec(
            f"""
            MATCH (n{ntype} {{ {properties} }})
            SET n:{label}
            RETURN n, labels(n) AS labels""")

    def create_relationship(self, name_a, type_a, properties, name_b, type_b):
        """ Create a relationship between two nodes given name and type for each end of the relationship and
        properties for the relationship itself. """
        relname = properties['name']
        rprops = ",".join([f""" {k} : "{v}" """ for k, v in properties.items() if not k == "name"])
        result = self.exec(
            f"""MATCH (a:{type_a} {{ name: "{name_a}" }})-[:{relname} {{ {rprops} }}]->(b:{type_b} {{ name : "{name_b}" }}) RETURN *""")
        return result if result.peek() else self.exec(
            f"""
            MATCH (a:{type_a} {{ name: "{name_a}" }})
            MATCH (b:{type_b} {{ name: "{name_b}" }})
            CREATE (a)-[:{relname} {{ {rprops} }}]->(b)""")
