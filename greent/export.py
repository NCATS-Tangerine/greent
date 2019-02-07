from greent import node_types
from greent.util import LoggingUtil,Text
from neo4j.v1 import GraphDatabase
from collections import defaultdict, deque
import calendar
import logging
from datetime import datetime
from sys import stdout

#watch("neo4j.bolt", logging.DEBUG, stdout)

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class BufferedWriter:
    """Buffered writer accepts individual nodes and edges to write to neo4j.
    It doesn't write the node/edge if it has already been written in its lifetime (it maintains a record)
    It then accumulates nodes/edges by label/type until a buffersize has been reached, at which point it does
    an intelligent update/write to the batch of nodes and edges.
    
    The correct way to use this is 
    with BufferedWriter(rosetta) as writer:
        writer.write_node(node)
        ...

    Doing this as a context manager will make sure that the different queues all get flushed out.
    """

    def __init__(self,rosetta):
        self.rosetta = rosetta
        self.written_nodes = set()
        self.written_edges = defaultdict(lambda: defaultdict( set ) )
        self.node_queues = defaultdict(list)
        self.edge_queues = defaultdict(list)
        self.node_buffer_size = 100
        self.edge_buffer_size = 100
        self.driver = self.rosetta.type_graph.driver

    def __enter__(self):
        return self

    def write_node(self,node):
        if node.id in self.written_nodes:
            return
        if node.name is None or node.name == '':
            logger.warning(f"Node {node.id} is missing a label")
        self.written_nodes.add(node.id)
        typednodes = self.node_queues[node.type]
        typednodes.append(node)
        if len(typednodes) >= self.node_buffer_size:
            self.flush()

    def write_edge(self,edge):
        if edge in self.written_edges[edge.source_id][edge.target_id]:
            return
        self.written_edges[edge.source_id][edge.target_id].add(edge)
        label = Text.snakify(edge.standard_predicate.label)
        typed_edges = self.edge_queues[label]
        typed_edges.append(edge)
        if len(typed_edges) >= self.edge_buffer_size:
            self.flush()

    def flush(self):
        with self.driver.session() as session:
            for node_type in self.node_queues:
                session.write_transaction(export_node_chunk,self.node_queues[node_type],node_type)
                self.node_queues[node_type] = []
            for edge_label in self.edge_queues:
                session.write_transaction(export_edge_chunk,self.edge_queues[edge_label],edge_label)
                self.edge_queues[edge_label] = []

    def __exit__(self,*args):
        self.flush()
        #Doesn't own the driver
        #self.driver.close()

def sort_edges_by_label(edges):
    el = defaultdict(list)
    deque( map( lambda x: el[Text.snakify(x[2]['object'].standard_predicate.label)].append(x), edges ) )
    return el

def export_edge_chunk(tx,edgelist,edgelabel):
    """The approach of updating edges will be to erase an old one and replace it in whole.   There's no real
    reason to worry about preserving information from an old edge.
    What defines the edge are the identifiers of its nodes, and the source.function that created it."""
    cypher = f"""UNWIND $batches as row
            MATCH (a:{node_types.ROOT_ENTITY} {{id: row.source_id}}),(b:{node_types.ROOT_ENTITY} {{id: row.target_id}})
            MERGE (a)-[r:{edgelabel} {{edge_source: row.provided_by, relation_label: row.original_predicate_label}}]->(b)
            set r.source_database=row.database
            set r.ctime=row.ctime 
            set r.predicate_id=row.standard_id 
            set r.relation=row.original_predicate_id 
            set r.publications=row.publications
            """
    batch = [ {'source_id': edge.source_id,
               'target_id': edge.target_id,
               'provided_by': edge.provided_by,
               'database': edge.provided_by.split('.')[0],
               'ctime': edge.ctime,
               'standard_id': edge.standard_predicate.identifier,
               'original_predicate_id': edge.original_predicate.identifier,
               'original_predicate_label': edge.original_predicate.label,
               'publication_count': len(edge.publications),
               'publications': edge.publications[:1000]
               }
              for edge in edgelist]

    tx.run(cypher,{'batches': batch})

    for edge in edgelist:
        if edge.standard_predicate.identifier == 'GAMMA:0':
            logger.warn(f"Unable to map predicate for edge {edge.original_predicate}  {edge}")

def sort_nodes_by_label(nodes):
    nl = defaultdict(list)
    deque( map( lambda x: nl[x.type].append(x), nodes ) )
    return nl


def export_node_chunk(tx,nodelist,label):
    cypher = f"""UNWIND $batches as batch
                MERGE (a:{node_types.ROOT_ENTITY} {{id: batch.id}})
                set a:{label}
                set a.name=batch.name
                set a.equivalent_identifiers=batch.synonyms
                set a += batch.properties
                """
    batch = []
    for n in nodelist:
        nodeout = { 'id': n.id, 'name': n.name, 'synonyms': [s.identifier for s in n.synonyms], 'properties': n.properties }
        batch.append(nodeout)
    tx.run(cypher,{'batches': batch})


