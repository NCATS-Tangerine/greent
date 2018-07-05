import logging
import traceback
import calendar
import json
import os

import requests
from collections import defaultdict
from greent.graph_components import KNode
from greent.util import LoggingUtil
from greent import node_types
from greent.export import BufferedWriter
from greent.cache import Cache
from builder.question import Node, Edge, LabeledThing

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class QueryDefinition:
    """Defines a query"""

    def __init__(self):
        self.start_values = None
        self.start_type = None
        self.end_values = None
        self.node_types = []
        self.transitions = []
        self.start_lookup_node = None
        self.end_lookup_node = None
        self.start_name = None
        self.end_name = None

class Program:

    def __init__(self, plan, nodes, rosetta, program_number):
        # Plan comes from typegraph and contains
        # transitions: a map from a node index to an (operation, output index) pair
        self.program_number = program_number
        self.concept_nodes = nodes
        self.transitions = plan
        self.rosetta = rosetta
        self.cache = Cache(redis_db=1)

        self.cache.flush()
        self.log_program()
        #self.excluded_identifiers=set()
        self.excluded_identifiers=set(['UBERON:0000468'])

        response = requests.get(f"{os.environ['FLOWER_BROKER_API']}queues/")
        queues = response.json()
        num_consumers = [q['consumers'] for q in queues if q['name'] == 'neo4j'][0]
        if num_consumers:
            import pika
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1',
                virtual_host='builder',
                credentials=pika.credentials.PlainCredentials('murphy', 'pword')))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='neo4j')
        else:
            self.connection = None
            self.channel = None

    def __del__(self):
        self.connection.close()

    def log_program(self):
        logstring = f'Program {self.program_number}\n'
        logstring += 'Nodes: \n'
        for i,cn in enumerate(self.concept_nodes):
            logstring+=f' {i}: {cn}\n'
        logstring += 'Transitions:\n'
        for k in self.transitions:
            logstring+=f' {k}: {self.transitions[k]}\n'
        logger.debug(logstring)

    def initialize_instance_nodes(self):
        # No error checking here. You should have caught any malformed questions before this point.
        logger.debug("Initializing program {}".format(self.program_number))
        for n in self.concept_nodes:
            if not n.curie:
                continue
            start_node = KNode(n.curie, n.type, label=n.name)
            start_node = Node(
                name=start_node.label,
                curie=start_node.identifier,
                type=start_node.node_type,
                synonyms={LabeledThing(
                        identifier=s.identifier,
                        label=s.label
                    ) for s in start_node.synonyms}
            )
            self.process_node(start_node, str(n.id))
        return

    def process_op(self, link, source_node, history):
        op_name = link['op']
        key = f"{op_name}({source_node.curie})"
        try:
            results = self.rosetta.cache.get(key)
            if results is not None:
                logger.debug(f"cache hit: {key} size:{len(results)}")
            else:
                logger.debug(f"exec op: {key}")
                op = self.rosetta.get_ops(op_name)
                synonyms = source_node.synonyms
                source_node = KNode(source_node.curie, source_node.type, label=source_node.name)
                source_node.synonyms = synonyms
                results = op(source_node)
                self.rosetta.cache.set(key, results)
                logger.debug(f"cache.set-> {key} length:{len(results)}")
                logger.debug(f"    {[node for _, node in results]}")
            for edge, node in results:
                node = Node(
                    name=node.label,
                    curie=node.identifier,
                    type=node.node_type,
                    synonyms={LabeledThing(
                        identifier=s.identifier,
                        label=s.label
                    ) for s in node.synonyms}
                )
                edge = Edge(
                    source_id=edge.subject_node.identifier,
                    target_id=edge.object_node.identifier,
                    standard_predicate=LabeledThing(
                        identifier=edge.standard_predicate.identifier,
                        label=edge.standard_predicate.label
                    ),
                    original_predicate=LabeledThing(
                        identifier=edge.original_predicate.identifier,
                        label=edge.original_predicate.label
                    ),
                    provided_by=edge.edge_source,
                    ctime=calendar.timegm(edge.ctime.timetuple()),
                    publications=edge.publications
                )
                self.process_node(node, history, edge)

        except Exception as e:
            traceback.print_exc()
            log_text = f"  -- {key}"
            logger.warning(f"Error invoking> {log_text}")

    def process_node(self, node, history, edge=None):
        """
        We've got a new set of nodes (either initial nodes or from a query).  They are attached
        to a particular concept in our query plan. We make sure that they're synonymized and then
        queue up their children
        """
        if edge is not None:
            is_source = node.curie == edge.source_id
        self.rosetta.synonymizer.synonymize(node)
        if edge is not None:
            if is_source:
                edge.source_id = node.curie
            else:
                edge.target_id = node.curie

        # check the node cache, compare to the provided history
        # to determine which ops are valid
        key = node.curie

        # print(node.dump())
        # if edge:
        #     print(edge.dump())
        print("-"*len(history)+"History: ", history)

        # only add a node if it wasn't cached
        completed = self.cache.get(key) # set of nodes we've been from here
        print("-"*len(history)+"Completed: ", completed)
        if completed is None:
            completed = set()
            self.cache.set(key, completed)

            if self.channel is None:
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_node(node)
            else:
                self.channel.basic_publish(exchange='',
                    routing_key='neo4j',
                    body=json.dumps({'nodes': [node.dump()], 'edges': []}))
            print(" [x] Sent node")

        # make sure the edge is queued for creation AFTER the node
        if edge:
            if self.channel is None:
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_edge(edge)
            else:
                self.channel.basic_publish(exchange='',
                    routing_key='neo4j',
                    body=json.dumps({'nodes': [], 'edges': [edge.dump()]}))
            print(" [x] Sent edge")

        # quit if we've closed a loop
        if history[-1] in history[:-1]:
            print("-"*len(history)+"Closed a loop!")
            return

        source_id = int(history[-1])

        # quit if there are no transitions from this node
        if source_id not in self.transitions:
            return

        destinations = self.transitions[source_id]
        completed = self.cache.get(key)
        for target_id in destinations:
            if not self.transitions[source_id][target_id]:
                continue
            # don't turn around
            if len(history)>1 and str(target_id) == history[-2]:
                continue
            # don't repeat things
            if target_id in completed:
                continue
            completed.add(target_id)
            self.cache.set(key, completed)
            links = self.transitions[source_id][target_id]
            print("-"*len(history)+f"Destination: {target_id}")
            for link in links:
                print("-"*len(history)+"Executing: ", link['op'])
                self.process_op(link, node, history+str(target_id))
        
    #CAN I SOMEHOW CAPTURE PATHS HERE>>>>

    def run_program(self):
        """Loop over unused nodes, send them to the appropriate operator, and collect the results.
        Keep going until there's no nodes left to process."""
        logger.debug(f"Running program {self.program_number}")
        self.initialize_instance_nodes()
        self.channel.basic_publish(exchange='',
            routing_key='neo4j',
            body='flush')
        return

    def get_path_descriptor(self):
        """Return a description of valid paths at the concept level.  The point is to have a way to
        find paths in the final graph.  By starting at one end of this, you can get to the other end(s).
        So it assumes an acyclic graph, which may not be valid in the future.  What it should probably
        return in the future (if we still need it) is a cypher query to find all the paths this program
        might have made."""
        path={}
        used = set()
        node_num = 0
        used.add(node_num)
        while len(used) != len(self.concept_nodes):
            next = None
            if node_num in self.transitions:
                putative_next = self.transitions[node_num]['to']
                if putative_next not in used:
                    next = putative_next
                    dir = 1
            if next is None:
                for putative_next in self.transitions:
                    ts = self.transitions[putative_next]
                    if ts['to'] == node_num:
                        next = putative_next
                        dir = -1
            if next is None:
                logger.error("How can this be? No path across the data?")
                raise Exception()
            path[node_num] = (next, dir)
            node_num = next
            used.add(node_num)
        return path
