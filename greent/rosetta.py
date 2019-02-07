import argparse
import json
import logging
import operator
import os
import re
import sys
import traceback
import yaml
from collections import defaultdict
from greent.servicecontext import ServiceContext
from greent.graph import Frame
from greent.graph import TypeGraph
from greent.graph_components import KNode, KEdge
from greent.identifiers import Identifiers
from greent.program import Program
from greent.program import QueryDefinition
from greent.synonymization import Synonymizer
#from greent.transreg import TranslatorRegistry
from greent.util import LoggingUtil
from greent.util import Text
from greent.annotators.annotator_factory import annotate_shortcut

logger = LoggingUtil.init_logging(__name__, level=logging.INFO)


class Rosetta:
    """ Rosetta's translates between semantic domains generically and automatically.
    Based on a configuration file, it builds a directed graph where types are nodes.
    Types are concepts from a model. Edges are annotated with the names of operators used
    to transition between the connected types. The engine can then accept requests to 
    translate a term from one domain to another. It does this by collecting transitions
    from the graph and executing the list of transitions. """

    def __init__(self, greentConf=None,
                 config_file=os.path.join(os.path.dirname(__file__), "rosetta.yml"),
                 delete_type_graph=False,
                 init_db=False,
                 build_indexes=False,
                 debug=False):

        """ The constructor loads the config file an prepares the type graph.
        If delete_type_graph flag is true, the graph is deleted entirely. 
        If the init_db flag is true, the type_graph will be loaded from the config file. """

        self.debug = False

        logger.debug("-- rosetta init.")
        self.service_context = ServiceContext(self,greentConf)
        self.core = self.service_context.core

        """ Load configuration. """
        with open(config_file, 'r') as stream:
            self.config = yaml.load(stream)
        self.operators = self.config["@operators"]
        self.type_checks = self.config["@type_checks"]

        # Abbreviation
        self.cache = self.service_context.cache  # core.service_context.cache

        """ Initialize type graph. """
        self.type_graph = TypeGraph(self.service_context, debug=debug)
        self.synonymizer = Synonymizer(self.type_graph.concept_model, self)

        """ Merge identifiers.org vocabulary into Rosetta vocab. """
        self.identifiers = Identifiers()

        if delete_type_graph:
            logger.debug("--Deleting type graph")
            self.type_graph.delete_all()

        if init_db:
            """ Initialize type graph metadata. """
            '''
            for k, v in self.identifiers.vocab.items():
                if isinstance(v, str):
                    self.type_graph.find_or_create(k, v)
            '''
            self.type_graph.find_or_create_list(self.identifiers.vocab.items())
            # self.configure_local_operators ()
            self.type_graph.configure_operators(self.operators.items())
            # self.configure_translator_registry ()
            self.type_graph.cast_edges(self.type_checks)

        if build_indexes:
            """Create neo4j indices for identifier on different labels"""
            self.type_graph.create_constraints()

    # deprecated
    def configure_local_operators(self):
        logger.debug("Configure operators in the Rosetta config.")
        for a_concept, transition_list in self.operators.items():
            for b_concept, transitions in transition_list.items():
                for transition in transitions:
                    link = transition['link']
                    op = transition['op']
                    self.create_concept_transition(a_concept, b_concept, link, op)

    def configure_translator_registry(self):
        logger.debug("Configure operators derived from the Translator Registry.")
        self.core.translator_registry = TranslatorRegistry(self.core.service_context)
        subscriptions = self.core.translator_registry.get_subscriptions()
        registrations = defaultdict(list)
        skip_patterns = list(map(lambda v: re.compile(v),
                                 self.config.get('@translator-registry', {}).get('skip_list', [])))
        for sub in subscriptions:
            in_concept = sub.in_concept
            out_concept = sub.out_concept
            op = f"translator_registry.{sub.op}"
            key = f"{in_concept}-{out_concept}-{op}"
            link = sub.predicate if sub.predicate else "unknown"
            link = link.upper()
            if any([p.match(sub.op) for p in skip_patterns]):
                logger.debug(f"==> Skipping registration of translator API {sub.op} based on configuration setting.")
                continue
            if key in registrations:
                continue
            registrations[key] = sub
            if not in_concept:
                logger.debug(f"Unable to find in concept for {sub}")
            elif not out_concept:
                logger.debug(f"Unable to find out concept for {sub}")
            else:
                if link and op:
                    self.create_concept_transition(in_concept, out_concept, link, op)

    def create_concept_transition(self, a_concept, b_concept, link, op):
        """ Create a link between two concepts in the type graph. """
        logger.debug("  -+ {} {} link: {} op: {}".format(a_concept, b_concept, link, op))
        try:
            self.type_graph.add_concepts_edge(a_concept, b_concept, predicate=link, op=op)
        except Exception as e:
            logger.error(f"Failed to create edge from {a_concept} to {b_concept} with link {link} and op {op}")
            logger.error(e)

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

    def construct_knowledge_graph(self, inputs, query):
        programs = self.type_graph.get_knowledge_map_programs(query)
        results = []
        for program in programs:
            print(f" program --**-->> {program}")
            results += self.execute_knowledge_graph_program(inputs, program)
        return results

    def execute_knowledge_graph_program(self, inputs, program):
        """ Construct a knowledge graph given a set of input nodes and a program - a list
        of frames, each of which contains the name of a concept, a collector containing a list of edges and
        nodes where all target nodes are instances of the frame's concept, and a list of operations for 
        transitioning from one frame's concept space to the next frames.

        This method assumes a linear path.
        """

        """ Convert inputs to be structured like edges-and-nodes returned by a previous services. """
        next_nodes = {key: [(None, KNode(val, type=key)) for val in val_list] for key, val_list in inputs.items()}
        logger.debug(f"inputs: {next_nodes}")

        """ Validated the input program. """
        if len(program) == 0:
            logger.info(f"No program found for {query}")
            return []
        logger.info(f"program> {program}")
        result = []

        """ Each frame's name is a concept. We use the top frame's as a key to index the arguments. """
        top_frame = program[0]
        inputs = next_nodes[top_frame.name]
        for i in inputs:
            self.synonymizer.synonymize(i[1])
            annotate_shortcut(i[1], self)

        """ Stack is the overall executable. We prepend a base frame with a collector primed with input arguments. """
        stack = [Frame(collector=inputs)] + program

        """ Execute the program frame by frame. """
        for index, frame in enumerate(program):
            # logger.debug (f"--inputs: {stack[index].collector}")
            for k, o in frame.ops.items():
                logger.debug(f"-- frame-index--> {frame} {index} {k}=>{o.op}")

            """ Process each node in the collector. """
            index = 0
            for edge, source_node in stack[index].collector:
                """ Process each operator in the frame. """
                for op_name, operator in frame.ops.items():

                    """ Generate a cache key. """
                    key = f"{operator.op}({source_node.id})"
                    try:
                        logger.debug(f"  --op: {key}")

                        """ Load the object from cache. """
                        response = self.cache.get(key)
                        if not response:

                            """ Invoke the knowledge source with the given input. """
                            op = self.get_ops(operator.op)
                            if not op:
                                raise Exception(f"Unable to find op: {operator.op}")
                            response = op(source_node)
                            for edge, node in response:

                                """ Process the edge adding metadata. """
                                if isinstance(edge, KEdge):
                                    edge.predicate = operator.predicate
                                    edge.source_node = source_node
                                    self.synonymizer.synonymize(node)
                                    annotate_shortcut(node,self)
                                    edge.target_node = node

                                """ Validate the id space of the returned data maps to the target concept. """
                                if index < len(program) - 1:
                                    target_concept_name = program[index + 1].name
                                    prefixes = self.type_graph.concept_model.get(target_concept_name).id_prefixes
                                    valid = any([node.id.upper().startswith(p.upper()) for p in prefixes])
                                    if not valid:
                                        logger.debug(
                                            f"Operator {operator} wired to type: {concept_name} returned node with id: {node.id}")

                            """ Cache the annotated and validated response. """
                            self.cache.set(key, response)

                        """ Add processed edges to the overall result. """
                        result += [edge for edge, node in response]
                        logger.debug(f"{key} => {Text.short(response)}")

                        """ Response edges go in the collector to become input for the next operation. """
                        frame.collector += response
                    except Exception as e:
                        traceback.print_exc()
                        logger.warning("Error invoking> {key}")
        logger.debug(f"returning {len(result)} values.")
        return result

    def get_knowledge_graph(self, inputs, query, ends=None):
        """ Handles two sided queries and direction changes. """
        print(f"query: {query}")
        print(f"inputs: {inputs}")
        print(f"ends:   {ends}")
        graph = []
        query_definition = QueryDefinition()
        query_definition.start_type = inputs["type"]
        query_definition.start_values = inputs["values"]
        query_definition.end_values = ends
        plans = self.type_graph.get_transitions(query)
        programs = [Program(plan, query_definition=query_definition, rosetta=self, program_number=i) for i, plan in
                    enumerate(plans)]
        for program in programs:
            g = program.run_program()
            graph += g
            # print(elements_to_json(g))
        return graph

    def n2chem(self, name):
        return self.core.ctd.drugname_string_to_drug_identifier(name) + \
               [x[0] for x in self.core.pharos.drugname_string_to_pharos_info(name)] + \
               ['PUBCHEM:{}'.format(r['drugID'].split('/')[-1]) for r in self.core.chembio.drugname_to_pubchem(name)]

    def n2disease(self, name):
        # This performs a case-insensitive exact match, and also inverts comma-ed names
        return self.core.mondo.search(name)


def execute_query(args, outputs, rosetta):
    """ Query rosetta. """
    blackboard = rosetta.construct_knowledge_graph(**args)
    """ Lower case all output values. """
    expect = list(map(lambda v: v.lower(), outputs['nodes']))
    """ Make a list of result ids. """
    ids = [e.target_node.id.lower() for e in blackboard]
    logger.debug(f"Received {len(ids)} nodes.")
    logger.debug(f"Expected {len(expect)} nodes.")
    logger.debug(f"  ==> ids: {ids}")
    matched = 0
    for o in expect:
        if o in ids:
            matched = matched + 1
        else:
            logger.error(f" {o} not in ids")
    assert matched == len(expect)
    return blackboard, ids


def test_disease_gene(rosetta):
    execute_query(**{
        "args": {
            "inputs": {
                "disease": [
                    "DOID:2841"
                ]
            },
            "query":
                """MATCH (a:disease),(b:gene), p = allShortestPaths((a)-[*]->(b))
                WHERE NONE (r IN relationships(p) WHERE type(r) = 'UNKNOWN' OR r.op is null) 
                RETURN p"""
        },
        "outputs": {
            "nodes": ['ncbigene:191585', 'ncbigene:2289', 'ncbigene:4057', 'ncbigene:1442', 'ncbigene:4843',
                      'ncbigene:165829', 'ncbigene:5739', 'ncbigene:79034', 'ncbigene:7031', 'ncbigene:1048',
                      'ncbigene:80206', 'ncbigene:6541', 'ncbigene:340547', 'ncbigene:55600', 'ncbigene:55076',
                      'ncbigene:9173', 'ncbigene:115362', 'ncbigene:85413', 'ncbigene:948', 'ncbigene:56521',
                      'ncbigene:2043', 'ncbigene:133308', 'ncbigene:1359', 'ncbigene:1475', 'ncbigene:1469',
                      'ncbigene:1803', 'ncbigene:6402', 'ncbigene:11254', 'ncbigene:5625', 'ncbigene:29126',
                      'ncbigene:137835', 'ncbigene:5744', 'ncbigene:10964', 'ncbigene:10085', 'ncbigene:6783',
                      'ncbigene:6318', 'ncbigene:7903', 'ncbigene:55107', 'ncbigene:3081', 'ncbigene:60437',
                      'ncbigene:1178', 'ncbigene:59340', 'ncbigene:7033', 'ncbigene:760', 'ncbigene:1470',
                      'ncbigene:3371', 'ncbigene:10631', 'ncbigene:6528', 'ncbigene:400823', 'ncbigene:117157',
                      'ncbigene:405753', 'ncbigene:154064', 'ncbigene:202333', 'ncbigene:150', 'ncbigene:1179',
                      'ncbigene:84830', 'ncbigene:2015', 'ncbigene:25803', 'ncbigene:5055', 'ncbigene:883',
                      'ncbigene:3171', 'ncbigene:202309', 'ncbigene:4915', 'ncbigene:51301', 'ncbigene:131450',
                      'ncbigene:26998', 'ncbigene:10344', 'ncbigene:6280', 'ncbigene:90102', 'ncbigene:2206',
                      'ncbigene:960', 'ncbigene:246', 'ncbigene:9982', 'ncbigene:9245', 'ncbigene:3550',
                      'ncbigene:50506', 'ncbigene:27306', 'ncbigene:8875']
        }
    },
                  rosetta=rosetta)


def test_drug_pathway(rosetta):
    execute_query(**{
        "args": {
            "inputs": {
                "chemical_substance": [
                    "DRUGBANK:DB00619"
                ]
            },
            "query":
                """MATCH (a:drug),(b:pathway), p = allShortestPaths((a)-[*]->(b)) 
                WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN' OR r.op is null) 
                RETURN p"""
        },
        "outputs": {
            "nodes": [
                "REACT:R-HSA-6799990"
            ]
        }
    },
                  rosetta=rosetta)


def test_fuzzy_query(rosetta):
    b = rosetta.get_knowledge_graph(**{
        "inputs": {
            "type": "chemical_substance",
            "values": ["MESH:D000068877"]
        },
        "query": """
        MATCH p=
        (c0:Concept {name: "chemical_substance" })
        --
        (c1:Concept {name: "gene" })
        --
        (c2:Concept {name: "biological_process" })
        --
        (c3:Concept {name: "cell" })
        FOREACH (n in relationships(p) | SET n.marked = TRUE)
        WITH p,c0,c3
        MATCH q=(c0:Concept)-[*0..3 {marked:True}]->(c3:Concept)
        WHERE p=q
        AND ALL( r in relationships(p) WHERE  EXISTS(r.op) )FOREACH (n in relationships(p) | SET n.marked = FALSE)
        RETURN p, EXTRACT( r in relationships(p) | startNode(r) )"""
    })
    print(b)


def test_two_sided_query(rosetta):
    b = rosetta.get_knowledge_graph(**{
        "inputs": {
            "type": "chemical_substance",
            "values": rosetta.n2chem("imatinib")
        },
        "ends": rosetta.n2disease("asthma"),
        "query": """
        MATCH p=
        (c0:Concept {name: "chemical_substance" })
        --
        (c1:Concept {name: "gene" })
        --
        (c2:Concept {name: "biological_process" })
        --
        (c3:Concept {name: "cell" })
        --
        (c4:Concept {name: "anatomical_entity" })
        --
        (c5:Concept {name: "phenotypic_feature" })
        --
        (c6:Concept {name: "disease" })
        FOREACH (n in relationships(p) | SET n.marked = TRUE)
        WITH p,c0,c6
        MATCH q=(c0:Concept)-[*0..6 {marked:True}]->()<-[*0..6 {marked:True}]-(c6:Concept)
        WHERE p=q
        AND ALL( r in relationships(p) WHERE  EXISTS(r.op) )FOREACH (n in relationships(p) | SET n.marked = FALSE)
        RETURN p, EXTRACT( r in relationships(p) | startNode(r) )"""
    })


def test_ebola(rosetta):
    b = rosetta.get_knowledge_graph(**{
        "inputs": {
            "type": "disease",
            "values": rosetta.n2disease("ebola")
        },
        "ends": None,
        "query":
            """MATCH p=
            (c0:Concept {name: "disease" })
            --
            (c1:Concept {name: "gene" })
            --
            (c2:Concept {name: "disease" })
            FOREACH (n in relationships(p) | SET n.marked = TRUE)
            WITH p,c0,c2
            MATCH q=(c0:Concept)-[*0..2 {marked:True}]->(c2:Concept)
            WHERE p=q
            AND ALL( r in relationships(p) WHERE  EXISTS(r.op) )FOREACH (n in relationships(p) | SET n.marked = FALSE)
            RETURN p"""
    })


def run_test_suite(rosetta):
    #    test_ebola(rosetta)
    test_two_sided_query(rosetta)


#    test_disease_gene (rosetta)
#    test_drug_pathway(rosetta)

def parse_args(args):
    parser = argparse.ArgumentParser(description='Rosetta.')
    parser.add_argument('--debug', help="Debug", action="store_true", default=False)
    parser.add_argument('--delete-type-graph',
                        help='Delete the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('--initialize-type-graph',
                        help='Build the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('--build-indexes', default=False, action='store_true', help='Create identifier indexes in neo4j')
    parser.add_argument('-t', '--test', help='Run tests.', action="store_true", default=False)
    parser.add_argument('-c', '--conf', help='GreenT config file to use.', default=None)
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.initialize_type_graph or args.delete_type_graph:
        rosetta = Rosetta(greentConf=args.conf,
                          init_db=args.initialize_type_graph,
                          delete_type_graph=args.delete_type_graph,
                          build_indexes=args.build_indexes,
                          debug=args.debug)
    if args.test:
        run_test_suite(Rosetta(greentConf=args.conf,
                               init_db=args.initialize_type_graph,
                               delete_type_graph=args.delete_type_graph,
                               debug=args.debug))
