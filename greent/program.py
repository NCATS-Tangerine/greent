import logging
import traceback
import calendar
import json
import os
import pickle
import pika
from datetime import datetime as dt
from datetime import timedelta
import hashlib

import requests
from collections import defaultdict
from greent.graph_components import KNode
from greent.util import LoggingUtil, Text
from greent import node_types
from greent.export import BufferedWriter
from greent.cache import Cache
from greent.graph_components import KEdge
from greent.annotators.annotator_factory import annotate_shortcut
import traceback
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

    def __init__(self, plan, machine_question, rosetta, program_number):
        # Plan comes from typegraph and contains
        # transitions: a map from a node index to an (operation, output index) pair
        self.program_number = program_number
        self.machine_question = machine_question
        self.transitions = plan
        self.rosetta = rosetta
        self.prefix = hashlib.md5((str(plan) + str(machine_question['nodes'])).encode()).hexdigest()
        self.cache = Cache(
            redis_host=os.environ['BUILD_CACHE_HOST'],
            redis_port=os.environ['BUILD_CACHE_PORT'],
            redis_db=os.environ['BUILD_CACHE_DB'],
            prefix=self.prefix)

        self.cache.flush()
        self.log_program()
        #self.excluded_identifiers=set()
        """
        EXCLUSION CANDIDATES:
        UBERON:0000468 multi-cellular organism
        UBERON:0001062 anatomical entity
        UBERON:0000479 tissue
        UBERON:0000062 organ
        UBERON:0000064 organ part
        UBERON:0000467 anatomical system
        UBERON:0000465 material anatomical entity 
        UBERON:0000061 anatomical structure
        UBERON:0010000 multicellular anatomical structure
        UBERON:0011216 organ system subdivision
        UBERON:0000475 organism subdivision
        0002405 immune system
        0001016 nervous system
        0001017 central nervous system
        0001007 digestive system
        0004535 cardiovascular system
        0000949 endocrine system
        0000079 male reproductive system
        0001434 skeletal system
        0000178 blood
        GO:0044267 cellular protein metabolic processes
        GO:0005515 protein binding
        CL:0000548 animal cell
        CL:0000003 native cell
        CL:0000255 eukaryotic cell
        """
        self.excluded_identifiers = {'UBERON:0000064','UBERON:0000475','UBERON:0011216','UBERON:0000062','UBERON:0000465','UBERON:0010000','UBERON:0000061', 'UBERON:0000467','UBERON:0001062','UBERON:0000468', 'UBERON:0000479', 'GO:0044267', 'GO:0005515', 'CL:0000548', 'CL:0000003', 'CL:0000255'}

        response = requests.get(f"{os.environ['BROKER_API']}queues/")
        queues = response.json()
        num_consumers = [q['consumers'] for q in queues if q['name'] == 'neo4j']
        if num_consumers and num_consumers[0]:
        #if False:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                heartbeat=0,
                host=os.environ['BROKER_HOST'],
                virtual_host='builder',
                credentials=pika.credentials.PlainCredentials(os.environ['BROKER_USER'], os.environ['BROKER_PASSWORD'])))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='neo4j')
        else:
            self.connection = None
            self.channel = None

    def __del__(self):
        if self.connection is not None:
            self.connection.close()

    def log_program(self):
        logstring = f'Program {self.program_number}\n'
        logstring += 'Nodes: \n'
        for i,cn in enumerate(self.machine_question['nodes']):
            logstring+=f' {i}: {cn}\n'
        logstring += 'Transitions:\n'
        for k in self.transitions:
            logstring+=f' {k}: {self.transitions[k]}\n'
        total_transitions = len(self.transitions.keys())
        if  total_transitions < 20:
            logger.debug(logstring)
        logger.debug(f'total transitions : {total_transitions}')

    def initialize_instance_nodes(self):
        # No error checking here. You should have caught any malformed questions before this point.
        logger.debug("Initializing program {}".format(self.program_number))
        for n in self.machine_question['nodes']:
            if not n.curie:
                continue
            start_node = KNode(n.curie, type=n.type, name=n.name)
            self.process_node(start_node, [n.id])
        return

    def process_op(self, link, source_node, history):
        op_name = link['op']
        key = f"{op_name}({Text.upper_curie(source_node.id)})"
        maxtime = timedelta(minutes=2)
        try:
            try:
                results = self.rosetta.cache.get(key)
            except Exception as e:
                # logger.warning(e)
                results = None
            if results is not None:
                logger.debug(f"cache hit: {key} size:{len(results)}")
            else:
                logger.debug(f"exec op: {key}")
                op = self.rosetta.get_ops(op_name)
                start = dt.now()
                results = op(source_node)
                end = dt.now()
                logger.debug(f'Call {key} took {end-start}')
                if (end-start) > maxtime:
                    logger.warn(f"Call {key} exceeded {maxtime}")
                self.rosetta.cache.set(key, results)
                logger.debug(f"cache.set-> {key} length:{len(results)}")
                logger.debug(f"    {[node for _, node in results]}")
            results = list(filter(lambda x: x[1].id not in self.excluded_identifiers, results))
            for edge, node in results:
                edge_label = Text.snakify(edge.standard_predicate.label)
                if link['predicate'] is None or edge_label == link['predicate']:
                    self.process_node(node, history, edge)
                else:
                    pass

        except pika.exceptions.ChannelClosed:
            raise
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
            is_source = node.id == edge.source_id
        self.rosetta.synonymizer.synonymize(node)
        try:
            result = annotate_shortcut(node, self.rosetta)
            if type(result) == type(None):
                logger.debug(f'No annotator found for {node}')
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
        if edge is not None:
            if is_source:
                edge.source_id = node.id
            else:
                edge.target_id = node.id

        # check the node cache, compare to the provided history
        # to determine which ops are valid
        key = node.id

        # print(node.dump())
        # if edge:
        #     print(edge.dump())
        #print("-"*len(history)+"History: ", history)

        # only add a node if it wasn't cached
        completed = self.cache.get(key) # set of nodes we've been from here
        #print("-"*len(history)+"Completed: ", completed)
        if completed is None:
            completed = set()
            self.cache.set(key, completed)

        if self.channel is None:
            with BufferedWriter(self.rosetta) as writer:
                writer.write_node(node)
        else:
            self.channel.basic_publish(
                exchange='',
                routing_key='neo4j',
                body=pickle.dumps({'nodes': [node], 'edges': []}))
        #logger.debug(f"Sent node {node.id}")

        # make sure the edge is queued for creation AFTER the node
        if edge:
            if self.channel is None:
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_edge(edge)
            else:
                self.channel.basic_publish(
                    exchange='',
                    routing_key='neo4j',
                    body=pickle.dumps({'nodes': [], 'edges': [edge]}))
            #logger.debug(f"Sent edge {edge.source_id}->{edge.target_id}")

        # quit if we've closed a loop
        if history[-1] in history[:-1]:
            #print("-"*len(history)+"Closed a loop!")
            return

        source_id = history[-1]

        # quit if there are no transitions from this node
        if source_id not in self.transitions:
            return

        destinations = self.transitions[source_id]
        completed = self.cache.get(key)
        for target_id in destinations:
            if not self.transitions[source_id][target_id]:
                continue
            # don't turn around
            if len(history)>1 and target_id == history[-2]:
                continue
            # don't repeat things
            if target_id in completed:
                continue
            completed.add(target_id)
            self.cache.set(key, completed)
            links = self.transitions[source_id][target_id]
            #print("-"*len(history)+f"Destination: {target_id}")
            for link in links:
                #print("-"*len(history)+"Executing: ", link['op'])
                self.process_op(link, node, history + [target_id])

    #CAN I SOMEHOW CAPTURE PATHS HERE>>>>

    def run_program(self):
        """Loop over unused nodes, send them to the appropriate operator, and collect the results.
        Keep going until there's no nodes left to process."""
        logger.debug(f"Running program {self.program_number}")
        self.initialize_instance_nodes()
        if self.channel is not None:
            self.channel.basic_publish(
                exchange='',
                routing_key='neo4j',
                body=pickle.dumps('flush'))
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
        while len(used) != len(self.machine_question['nodes']):
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
