import argparse
import logging
import operator
import os
import sys
import traceback
import yaml
import requests_cache
from collections import defaultdict
from greent.transreg import TranslatorRegistry
from greent.identifiers import Identifiers
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import Text
from greent.util import DataStructure
from greent.graph import TypeGraph
from greent.graph_components import KNode, KEdge
from greent.synonymization import Synonymizer
from neo4jrestclient.exceptions import StatusException

logger = LoggingUtil.init_logging(__file__, level=logging.INFO)

class Rosetta:
    """ Rosetta's translates between semantic domains generically and automatically.
    Based on a configuration file, it builds a directed graph where types are nodes.
    Types can be IRIs or CURIEs. Edges are annotated with the names of operators used
    to transition between the connected types. The engine can then accept requests to 
    translate a term from one domain to another. It does this by collecting transitions
    from the graph and executing the list of transitions. """

    def __init__(self, greentConf="greent.conf",
                 config_file=os.path.join(os.path.dirname(__file__), "rosetta.yml"),
                 override={},
                 delete_type_graph=False,
                 init_db=False,
                 debug=False):

        """ The constructor loads the config file an prepares the type graph. If the delete_type_graph 
        flag is true, the graph is deleted entirely. If the init_db flag is true, the type_graph will
        be loaded from the config file. """
        """ Load the config file and set up a DiGraph representing the types we know 
        about and how to transition between them. """
        from greent.core import GreenT
        self.debug = False
        self.cache_path = 'rosetta_cache'

        logger.debug("-- Initialize GreenT service core.")
        self.core = GreenT(config=greentConf, override=override)

        logger.debug("-- Loading Rosetta graph schematic config: {0}".format(config_file))
        with open(config_file, 'r') as stream:
            self.config = yaml.load(stream)
        self.concepts = self.config["@concepts"]
        self.operators = self.config["@operators"]
        
        self.synonymizer = Synonymizer( self.config, self.core )

        logger.debug("-- Initializing Rosetta type graph")
        self.type_graph = TypeGraph(self.core.service_context, debug=debug)

        logger.debug("-- Merge Identifiers.org vocabulary into Rosetta vocab.")
        self.identifiers = Identifiers ()
        if delete_type_graph:
            logger.debug("--Deleting type graph")
            self.type_graph.delete_all()

        if init_db:
            logger.debug("--Initialize concept graph metadata and create type nodes.")
            for k, v in self.identifiers.vocab.items():
                if isinstance(v, str):
                    self.type_graph.find_or_create(k, v)
            self.configure_local_operators ()
            #self.configure_translator_registry ()
            
    def configure_local_operators (self):
        logger.debug ("Configure operators in the Rosetta config.")
        logger.debug ("""
    ____                  __  __       
   / __ \____  ________  / /_/ /_____ _
  / /_/ / __ \/ ___/ _ \/ __/ __/ __ `/
 / _, _/ /_/ (__  /  __/ /_/ /_/ /_/ / 
/_/ |_|\____/____/\___/\__/\__/\__,_/  
                                       """)
        for a_concept, transition_list in self.operators.items ():
            for b_concept, transitions in transition_list.items ():
                for transition in transitions:
                    link = transition['link']
                    op   = transition['op']
                    self.create_concept_transition (a_concept, b_concept, link, op)
                    
    def configure_translator_registry (self):
        logger.debug ("Configure operators derived from the Translator Registry.")
        logger.debug ("""
  ______                      __      __                ____             _      __            
 /_  ___________ _____  _____/ ____ _/ /_____  _____   / __ \___  ____ _(______/ /________  __
  / / / ___/ __ `/ __ \/ ___/ / __ `/ __/ __ \/ ___/  / /_/ / _ \/ __ `/ / ___/ __/ ___/ / / /
 / / / /  / /_/ / / / (__  / / /_/ / /_/ /_/ / /     / _, _/  __/ /_/ / (__  / /_/ /  / /_/ / 
/_/ /_/   \__,_/_/ /_/____/_/\__,_/\__/\____/_/     /_/ |_|\___/\__, /_/____/\__/_/   \__, /  
                                                               /____/                /____/   """)
        self.core.translator_registry = TranslatorRegistry(self.core.service_context)
        subscriptions = self.core.translator_registry.get_subscriptions()
        registrations = defaultdict(list)
        for sub in subscriptions:
            in_concept = sub.in_concept
            out_concept = sub.out_concept
            op = f"translator_registry.{sub.op}"
            key = f"{in_concept}-{out_concept}-{op}"
            link = sub.predicate if sub.predicate else "unknown"
            link = link.upper()
            if key in registrations:
                continue
            registrations [key] = sub
            if not in_concept:
                logger.debug(f"Unable to find in concept for {sub}")
            elif not out_concept:
                logger.debug(f"Unable to find out concept for {sub}")
            else:
                if link and op:
                    self.create_concept_transition (in_concept, out_concept, link, op)

    def create_concept_transition (self, a_concept, b_concept, link, op):
        """ Create a link between two concepts in the type graph. """
        logger.debug ("  -+ {} {} link: {} op: {}".format(a_concept, b_concept, link, op))
        try:
            self.type_graph.add_concepts_edge(a_concept, b_concept, predicate=link, op=op)
        except StatusException:
            logger.error(f"Failed to create edge from {a_concept} to {b_concept} with link {link} and op {op}")
            
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

    def graph(self, next_nodes, query):
        """ Given a set of starting nodes and a query, execute the query to get a set of paths.
        Each path reflects a set of transitions from the starting tokens through the graph.
        Each path is then executed and the resulting links and nodes returned. """
        programs = self.type_graph.get_transitions(query)
        logger.debug (f"-- programs: {programs}")
        result = []
        for program in programs:
            result += self.graph_inner(next_nodes, program)
        return result

    def graph_inner(self, next_nodes, program):
        import json
        if not program or len(program) == 0:
            return []
        primed = [{'collector': next_nodes}] + program
        linked_result = []
        for index, level in enumerate(program):
            logger.debug("--Executing level: {0}".format(level))
            operators = level['ops']
            collector = level['collector']
            for edge_node in primed[index]['collector']:
                for operator in operators:
                    op = self.get_ops(operator['op'])
                    try:
                        results = None
                        log_text = "  -- {0}({1})".format(operator['op'], edge_node[1].identifier)
                        source_node = edge_node[1]
                        with requests_cache.enabled("rosetta_cache"):
                            results = op(source_node)
                        for r in results:
                            edge = r[0]
                            if isinstance(edge, KEdge):
                                edge.predicate = operator['link']
                                edge.source_node = source_node
                                self.synonymizer.synonymize(r[1])
                                edge.target_node = r[1]
                                linked_result.append(edge)
                        logger.debug("{0} => {1}".format(log_text, Text.short(results)))
                        for r in results:
                            if index < len(program) - 1:
                                if not r[1].identifier.startswith(program[index + 1]['node_type']):
                                    logger.debug(
                                        "Operator {0} wired to return type: {1} returned node with id: {2}".format(
                                            operator, program[index + 1]['node_type'], r[1].identifier))
                        collector += results
                    except Exception as e:
                        traceback.print_exc()
                        logger.error("Error invoking> {0}".format(log_text))
        return linked_result

    def clinical_outcome_pathway(self, drug=None, disease=None):
        blackboard = []
        from greent import node_types
        if disease:
            blackboard += self.graph(
                [(None, KNode(f"NAME.DISEASE:{disease}", node_types.DISEASE_NAME))],
                query= """MATCH (n:named_thing)-[a]->(d:disease)-[b]->(g:gene) RETURN *""")
            blackboard += self.graph(
                [(None, KNode('NAME.DISEASE:{0}'.format(disease), node_types.DISEASE))],
                query= \
                    """MATCH (a{name:"NAME.DISEASE"}),(b:Gene), p = allShortestPaths((a)-[*]->(b)) 
                    WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN') 
                    RETURN p""")
        if drug:
            blackboard += self.graph(
                [(None, KNode('NAME.DRUG:{0}'.format(drug), node_types.DRUG_NAME))],
                query= \
                    """MATCH (a{name:"NAME.DRUG"}),(b:Pathway), p = allShortestPaths((a)-[*]->(b)) 
                    WHERE NONE (r IN relationships(p) WHERE type(r)='UNKNOWN') 
                    RETURN p""")
        return blackboard

    @staticmethod
    def clinical_outcome_pathway_app(drug=None, disease=None, greent_conf='greent.conf', debug=False):
        return Rosetta(greentConf=greent_conf, debug=debug).clinical_outcome_pathway(drug=drug, disease=disease)

    @staticmethod
    def clinical_outcome_pathway_app_from_args(args, greent_conf='greent.conf'):
        result = []
        if isinstance(args, list) and len(args) == 2 and \
                isinstance(args[0], str) and isinstance(args[1], str):
            result = (
                args,
                Rosetta.clinical_outcome_pathway_app(
                    drug=args[0],
                    disease=args[1],
                    greent_conf=greent_conf))
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rosetta.')
    parser.add_argument('--debug', help="Debug", action="store_true", default=False)
    parser.add_argument('--delete-type-graph',
                        help='Delete the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('--initialize-type-graph',
                        help='Build the graph of types and semantic transitions between them.',
                        action="store_true", default=False)
    parser.add_argument('-d', '--disease', help='A disease to analyze.', default=None)
    parser.add_argument('-s', '--drug', help='A drug to analyze.', default=None)
    args = parser.parse_args()

    if args.debug:
        logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)

    if args.initialize_type_graph or args.delete_type_graph:
        rosetta = Rosetta(init_db=args.initialize_type_graph,
                          delete_type_graph=args.delete_type_graph,
                          debug=args.debug)
    else:
        blackboard = Rosetta.clinical_outcome_pathway_app(drug=args.drug,
                                                          disease=args.disease,
                                                          debug=args.debug)
        print("output: {}".format(blackboard))



    
