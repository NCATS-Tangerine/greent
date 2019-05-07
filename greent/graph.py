import json
import logging
import sys
import traceback
from collections import defaultdict

from greent.concept import Concept
from greent.concept import ConceptModel
from greent.node_types import ROOT_ENTITY
from greent.service import Service
from greent.util import LoggingUtil
from neo4j.v1 import GraphDatabase

logger = logging.getLogger(__name__)

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
        self.edges_by_source = defaultdict(list)
        self.edges_by_target = defaultdict(list)
        self.base_op_to_concepts = defaultdict(list)
        self.concept_model_name = concept_model_name
        self.set_concept_model()
        self.TYPE = "Type"
        self.CONCEPT = "Concept"
        config = self.get_config()
        self.driver = GraphDatabase.driver(self.url, auth=("neo4j", config['neo4j_password']))

    def initialize_connection(self):
        """ Connect to the database. """
        config = self.get_config()
        logger.debug(f"  -+ Connecting to graph database: {self.url}")

    def delete_all(self):
        """ Delete the type-graph only.  Leave result graphs alone. """
        try:
            with self.driver.session() as session:
                db = GraphDB(session)
                db.exec("MATCH (n:Concept) DETACH DELETE n")
                db.exec("MATCH (n:Type) DETACH DELETE n")
        except Exception as e:
            traceback.print_exc()

    def create_constraints(self):
        """Neo4j demands that constraints are by label.  That is, you might have a constraint that
        every anatomy label occurs uniquely, or every cell label.  But there's not a way to say that
        every identifier is unique across all of the nodes except to give them all a common parent
        label. In biolink-model, that parent entity is named_thing, so that is what we will call it."""
        try:
            with self.driver.session() as session:
                db = GraphDB(session)
                concepts = db.exec(f"MATCH (c:Concept) return c.name as name")
                for concept in concepts:
                    db.exec(f"CREATE CONSTRAINT ON (p:{concept['name']}) ASSERT p.id IS UNIQUE")
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

    def build_concept(self, db, concept):
        """ Build a concept and its semantic backstory including is_a hierarcy. """
        if concept:
            self._find_or_create_concept(db, concept)
            if concept.is_a:
                """ If it has an ancestor, create a node for the ancestor and link the nodes. """
                base_class = self._find_or_create_concept(db, concept.is_a)
                db.create_relationship(
                    name_a=concept.name, type_a=self.CONCEPT,
                    properties={
                        "name": "is_a"
                    },
                    name_b=concept.is_a.name, type_b=self.CONCEPT)
            """ Recurse. """
            self.build_concept(db, concept.is_a)

    def find_or_create_list(self, items):
        with self.driver.session() as session:
            db = GraphDB(session)
            for k, v in items:
                if isinstance(v, str):
                    self.find_or_create(db, k, v)

    # make private
    def find_or_create(self, db, name, iri=None):
        """ Find a type node, creating it if necessary. Link it to a concept. """
        concept = self.type_to_concept.get(name)
        if concept:
            logger.error(f"   adding node {name} to concept {concept.name}")
            self._find_or_create_concept(db, concept)
            self.build_concept(db, concept)

    def configure_operators (self, operators):
        with self.driver.session() as session:
            db = GraphDB(session)
            logger.debug ("Configure operators in the Rosetta config.")
            for a_concept, transition_list in operators:
                for b_concept, transitions in transition_list.items ():
                    for transition in transitions:
                        link = transition['link']
                        op   = transition['op']
                        self.create_concept_transition (db, a_concept, b_concept, link, op)
                    
    def create_concept_transition (self, db, a_concept, b_concept, link, op):
        """ Create a link between two concepts in the type graph. """
        logger.debug ("  -+ {} {} link: {} op: {}".format(a_concept, b_concept, link, op))
        print (f"  -+ {a_concept} {b_concept} link: {link} op: {op}")
        try:
            self.add_concepts_edge(db, a_concept, b_concept, predicate=link, op=op)
        except Exception as e:
            logger.error(f"Failed to create edge from {a_concept} to {b_concept} with link {link} and op {op}")
            logger.error(e)
            
    def add_concepts_edge(self, db, a, b, predicate, op, base_op = None):
        """ Add an edge between two concepts. Include the operation to call to effect the transition. """
        a_concept = self.concept_model.get(a)
        b_concept = self.concept_model.get(b)
        assert a_concept, f"Unable to find concept {a}"
        assert b_concept, f"Unable to find concept {b}"
        a_concept_node = self._find_or_create_concept(db, a_concept)
        b_concept_node = self._find_or_create_concept(db, b_concept)
        db.create_relationship(name_a=a_concept.name, type_a=self.CONCEPT,
                               properties={
                                   "name": predicate,
                                   "predicate": predicate,
                                   "op": op,
                                   "enabled": True
                               },
                               name_b=b_concept.name, type_b=self.CONCEPT)
        if base_op == None:
            base_op = op
        edge = {'source': a_concept.name, 'target': b_concept.name, 'predicate': predicate, 'op': op, 'base_op': base_op}
        self.edges_by_source[a_concept.name].append(edge)
        self.edges_by_target[b_concept.name].append(edge)
        self.base_op_to_concepts[base_op].append( (a_concept.name, b_concept.name) )
        return edge

    def _find_or_create_concept(self, db, concept):
        """ Find or create a concept object which will be linked to member type object. """
        concept_node = None
        try:
            properties = {"name": concept.name}
            result = db.get_node(properties, node_type=self.CONCEPT)
            concept_node = result.peek()
            if not concept_node:
                concept_node = db.create_node(properties, node_type=self.CONCEPT)
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
        with self.driver.session() as session:
            db = GraphDB(session)
            usable_concepts = self.get_concepts_with_edges()
            children= self._push_up(db, type_check_functions,usable_concepts)
            self._pull_down(db, children, type_check_functions )

    def _push_up(self, db, type_check_functions, usable_concepts):
        this_level = self.concept_model.get_leaves()
        children = defaultdict(list)
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
                for edge in self.edges_by_source[concept.name]:
                    cop = self.create_caster_op(edge['op'])
                    if concept.name in type_check_functions:
                        newop = self.wrap_op('input_filter', cop, concept.name, type_check_functions[concept.name])
                    else:
                        newop = self.wrap_op('input_filter', cop, concept.name)
                    if (parent.name, edge['target']) in self.base_op_to_concepts[edge['base_op']]:
                        continue #already have it, don't need it again
                    newedge = self.add_concepts_edge(db, parent.name, edge['target'], edge['predicate'], newop, edge['base_op'])
                for edge in self.edges_by_target[concept.name]:
                    cop = self.create_caster_op(edge['op'])
                    newop = self.wrap_op('upcast', cop, parent.name)
                    if (edge['source'],parent.name) in self.base_op_to_concepts[edge['base_op']]:
                        continue #already have it, don't need it again
                    newedge = self.add_concepts_edge(db, edge['source'], parent.name, edge['predicate'], newop, edge['base_op'])
            this_level = next_level
        return children

    def _pull_down(self, db, children_dict, type_check_functions):
        this_level = self.concept_model.get_roots()
        while len(this_level) > 0:
            next_level = set()
            for concept in this_level:
                children = children_dict[concept]
                next_level.update(children)
                for edge in self.edges_by_source[concept.name]:
                    for child in children:
                        if (child.name,edge['target']) in self.base_op_to_concepts[edge['base_op']]:
                            continue #already have it, don't need it again
                        # taking parent, nothing else really required
                        newedge = self.add_concepts_edge(db, child.name, edge['target'], edge['predicate'], edge['op'], edge['base_op'])
                for edge in self.edges_by_target[concept.name]:
                    cop = self.create_caster_op(edge['op'])
                    for child in children:
                        if (edge['source'],child.name) in self.base_op_to_concepts[edge['base_op']]:
                            continue
                        try:
                            newop = self.wrap_op('output_filter', cop, child.name, type_check_functions[child.name])
                            self.add_concepts_edge(db, edge['source'], child.name, edge['predicate'], newop, edge['base_op'])
                        except KeyError:
                            pass
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
        if oldop.startswith('caster'):
            return oldop
        return 'caster.{}'.format('~'.join(oldop.split('.')))

    def run_cypher_query(self, query):
        """ Execute a cypher query and return the result set. """
        result = None
        try:
            with self.driver.session() as session:
                db = GraphDB(session)
                #result = db.query(query, data_contents=True)
                result = db.query(query) 
        except Exception:
            print("Error Generated by:")
            print(query)
            result = None
        return result

    def get_knowledge_map_programss(self, query):
        result = []
        with self.driver.session() as session:
            db = GraphDB(session)
            result = self.get_knowledge_map_programs_actor(db, query)
        return result

    def get_knowledge_map_programs_actor(self, db, query):
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
        result = db.query(query)
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
        sedges = set(self.edges_by_source.keys())
        tedges = set(self.edges_by_target.keys())
        sedges.update(tedges)
        return list(sedges)


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
        
    def __del__(self):
        self.session.close()

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


class Rel:
    def __init__(self,start,end):
        self.start = start
        self.end = end
    def __repr__(self):
        return f"start: {self.start} end: {self.end}"
