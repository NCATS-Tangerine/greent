import logging
import traceback
from collections import defaultdict
from greent.graph_components import KNode
from greent.util import LoggingUtil
from greent import node_types
from greent.export import BufferedWriter

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

    def __init__(self, plan, query_definition, rosetta, program_number):
        #Plan comes from typegraph and contains two things:
        #0: nodes: A map from a node index to the concept.
        #1: transitions: a map from a node index to an (operation, output index) pair
        #So the plan does not have to represent a linear query
        self.program_number = program_number
        self.concept_nodes = plan[0]
        self.transitions = plan[1]
        self.rosetta = rosetta
        self.unused_instance_nodes = set()
        self.all_instance_nodes = set()
        #self.initialize_instance_nodes(query_definition)
        self.linked_results = []
        #hang onto this for the moment.
        self.query_definition = query_definition
        self.start_nodes = []
        self.end_nodes = []
        self.check_program(query_definition)
        self.log_program()

    def check_program(self,qd):
        #Check for lines - need to have outward edges from defined nodes
        if 0 not in self.transitions:
            raise Exception("No transition from start node?")
        if qd.end_values is not None:
            last_node = len(self.concept_nodes)-1
            if last_node not in self.transitions:
                raise Exception("No transition from end node?")

    def log_program(self):
        logstring = f'Program {self.program_number}\n'
        logstring += 'Nodes: \n'
        for i,cn in enumerate(self.concept_nodes):
            logstring+=f' {i}: {cn}\n'
        logstring += 'Transitions:\n'
        for k in self.transitions:
            logstring+=f' {k}: {self.transitions[k]}\n'
        logger.debug(logstring)

    def initialize_instance_nodes(self,writer):
        logger.debug("Initializing program {}".format(self.program_number))
        t_node_ids = self.get_fixed_concept_nodes()
        self.start_nodes = [KNode(start_identifier, self.concept_nodes[t_node_ids[0]],label=self.query_definition.start_name)
                            for start_identifier in self.query_definition.start_values]
        self.add_instance_nodes(self.start_nodes, t_node_ids[0], writer)
        if len(t_node_ids) == 1:
            if self.query_definition.end_values:
                raise Exception(
                    "We only have one set of fixed nodes in the query plan, but multiple sets of fixed instances")
            return
        if len(t_node_ids) == 2:
            if not self.query_definition.end_values:
                raise Exception(
                    "We have multiple fixed nodes in the query plan but only one set of fixed instances")
            self.end_nodes = [KNode(start_identifier, self.concept_nodes[t_node_ids[-1]], label=self.query_definition.end_name)
                              for start_identifier in self.query_definition.end_values]
            self.add_instance_nodes(self.end_nodes, t_node_ids[-1], writer)
            return
        raise Exception("We don't yet support more than 2 instance-specified nodes")

    def get_fixed_concept_nodes(self):
        """Fixed concept nodes are those that only have outputs"""
        nodeset = set(self.transitions.keys())
        for transition in self.transitions.values():
            nodeset.discard(transition['to']) #Discard doesn't raise error if 'to' not in nodeset
        fixed_node_identifiers = list(nodeset)
        fixed_node_identifiers.sort()
        return fixed_node_identifiers

    def add_instance_nodes(self, nodelist, context, writer):
        """We've got a new set of nodes (either initial nodes or from a query).  They are attached
        to a particular concept in our query plan. We make sure that they're synonymized and then
        add them to both all_instance_nodes as well as the unused_instance_nodes"""
        for node in nodelist:
            self.rosetta.synonymizer.synonymize(node)
            node.add_context(self.program_number, context)
        self.all_instance_nodes.update(nodelist)
        self.unused_instance_nodes.update([(node, context) for node in nodelist])
        for node in nodelist:
            writer.write_node(node)
        
    #CAN I SOMEHOW CAPTURE PATHS HERE>>>>

    def run_program(self):
        """Loop over unused nodes, send them to the appropriate operator, and collect the results.
        Keep going until there's no nodes left to process."""
        logger.debug(f"Running program {self.program_number}")
        with BufferedWriter(self.rosetta) as writer:
            self.initialize_instance_nodes(writer)
            while len(self.unused_instance_nodes) > 0:
                source_node, context = self.unused_instance_nodes.pop()
                if context not in self.transitions:
                    #Need a comment explaining why this is here.
                    continue
                link = self.transitions[context]
                next_context = link['to']
                op_name = link['op']
                key = f"{op_name}({source_node.identifier})"
                log_text = f"  -- {key}"
                try:
                    results = self.rosetta.cache.get (key)
                    if results is not None:
                        logger.info (f"cache hit: {key} size:{len(results)}")
                        #When we get an edge out of the cache, it stores the old source node in it.
                        #Because context is in our copy of the source node, this can cause problems
                        # in support.   So we need to replace the cached source with our source node
                        for edge,other in results:
                            if edge.subject_node.identifier == source_node.identifier:
                                edge.subject_node = source_node
                            elif edge.object_node.identifier == source_node.identifier:
                                edge.object_node = source_node
                            else:
                                logger.error("Cached edge doesn't have source node in it")
                                raise Exception("Cached edge doesn't have source node in it")
                    else:
                        logger.info (f"exec op: {key}")
                        op = self.rosetta.get_ops(op_name)
                        results = op(source_node)
                        self.rosetta.cache.set (key, results)
                        logger.debug (f"cache.set-> {key} length:{len(results)}")
                    newnodes = []
                    for r in results:
                        edge = r[0]
                        self.linked_results.append(edge)
                        writer.write_edge(edge)
                        newnodes.append(r[1])
                    logger.debug(f"    {newnodes}")
                    self.add_instance_nodes(newnodes,next_context,writer)
                except Exception as e:
                    traceback.print_exc()
                    logger.warning(f"Error invoking> {log_text}")
                logger.debug(f" {len(self.unused_instance_nodes)} nodes remaining.")
        return self.linked_results

    def get_results(self):
        return self.linked_results

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
