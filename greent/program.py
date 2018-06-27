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

    def __init__(self, plan, nodes, rosetta, program_number):
        # Plan comes from typegraph and contains
        # transitions: a map from a node index to an (operation, output index) pair
        self.program_number = program_number
        self.concept_nodes = nodes
        self.transitions = plan
        self.rosetta = rosetta
        self.unused_instance_nodes = set()
        self.all_instance_nodes = set()
        self.linked_results = []
        self.log_program()

    def log_program(self):
        logstring = f'Program {self.program_number}\n'
        logstring += 'Nodes: \n'
        for i,cn in enumerate(self.concept_nodes):
            logstring+=f' {i}: {cn}\n'
        logstring += 'Transitions:\n'
        for k in self.transitions:
            logstring+=f' {k}: {self.transitions[k]}\n'
        logger.debug(logstring)

    def initialize_instance_nodes(self, writer):
        # No error checking here. You should have caught any malformed questions before this point.
        logger.debug("Initializing program {}".format(self.program_number))
        for n in self.concept_nodes:
            if not n.curie:
                continue
            start_nodes = [KNode(n.curie, n.type, label=n.name)]
            self.add_instance_nodes(start_nodes, n.id, writer)
        return

    def add_instance_nodes(self, nodelist, source_id, writer):
        """We've got a new set of nodes (either initial nodes or from a query).  They are attached
        to a particular concept in our query plan. We make sure that they're synonymized and then
        add them to both all_instance_nodes as well as the unused_instance_nodes"""
        for node in nodelist:
            self.rosetta.synonymizer.synonymize(node)
            node.add_context(self.program_number, source_id)
        self.all_instance_nodes.update(nodelist)
        self.unused_instance_nodes.update([(node, source_id) for node in nodelist])
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
                source_node, source_id = self.unused_instance_nodes.pop()
                if source_id not in self.transitions:
                    # there are no transitions from this node
                    continue
                links = self.transitions[source_id]
                for link in links:
                    next_source_id = link['target_id']
                    op_name = link['op']
                    key = f"{op_name}({source_node.identifier})"
                    log_text = f"  -- {key}"
                    try:
                        results = self.rosetta.cache.get(key)
                        if results is not None:
                            logger.info (f"cache hit: {key} size:{len(results)}")
                            # When we get an edge out of the cache, it stores the old source node in it.
                            # Because source_id is in our copy of the source node, this can cause problems
                            #   in support. So we need to replace the cached source with our source node.
                            for edge, other in results:
                                if edge.subject_node.identifier == source_node.identifier:
                                    edge.subject_node = source_node
                                elif edge.object_node.identifier == source_node.identifier:
                                    edge.object_node = source_node
                                else:
                                    logger.error("Cached edge doesn't have source node in it")
                                    raise Exception("Cached edge doesn't have source node in it")
                        else:
                            logger.info(f"exec op: {key}")
                            op = self.rosetta.get_ops(op_name)
                            results = op(source_node)
                            self.rosetta.cache.set (key, results)
                            logger.debug(f"cache.set-> {key} length:{len(results)}")
                        newnodes = []
                        for r in results:
                            edge = r[0]
                            self.linked_results.append(edge)
                            writer.write_edge(edge)
                            newnodes.append(r[1])
                        logger.debug(f"    {newnodes}")
                        self.add_instance_nodes(newnodes, next_source_id, writer)
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
