from greent.graph_components import KNode, KEdge
from greent import node_types
from greent.util import LoggingUtil,Text
from neo4j.v1 import GraphDatabase
from collections import defaultdict, deque
import calendar
import logging
from neo4j.util import watch
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
        if node.identifier not in self.written_nodes:
            if node.label is None or node.label == '':
                logger.error(f"Node {node.identifier} is missing a label")
            self.written_nodes.add(node.identifier)
            typednodes = self.node_queues[node.node_type]
            typednodes.append(node)
            if len(typednodes) >= self.node_buffer_size:
                with self.driver.session() as session:
                    session.write_transaction(export_node_chunk,typednodes,node.node_type)
                self.node_queues[node.node_type] = []

    def write_edge(self,edge):
        if edge not in self.written_edges[edge.subject_node][edge.object_node]:
            self.written_edges[edge.subject_node][edge.object_node].add(edge)
            label = Text.snakify(edge.standard_predicate.label)
            typed_edges = self.edge_queues[label]
            typed_edges.append(edge)
            if len(typed_edges) >= self.edge_buffer_size:
                with self.driver.session() as session:
                    session.write_transaction(export_edge_chunk,typed_edges,label)
                self.edge_queues[label] = []

    def __exit__(self,*args):
        with self.driver.session() as session:
            for node_type in self.node_queues:
                session.write_transaction(export_node_chunk,self.node_queues[node_type],node_type)
            for edge_label in self.edge_queues:
                session.write_transaction(export_edge_chunk,self.edge_queues[edge_label],edge_label)
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
    cypher = """UNWIND {batches} as row
            MATCH (a:%s {id: row.aid}),(b:%s {id: row.bid})
            MERGE (a)-[r:%s {edge_source: row.edge_source}]-(b)
            set r.source_database=row.database
            set r.ctime=row.ctime 
            set r.predicate_id=row.standard_id 
            set r.relation_label=row.original_predicate_label
            set r.relation=row.original_predicate_id 
            set r.publications=row.publications
            set r.url=row.url
            set r.input_identifiers=row.input
            """ % (node_types.ROOT_ENTITY, node_types.ROOT_ENTITY, edgelabel)
    batch = [ {'aid': edge.subject_node.identifier,
               'bid': edge.object_node.identifier,
               'edge_source': edge.edge_source,
               'database': edge.edge_source.split('.')[0],
               'ctime': calendar.timegm(edge.ctime.timetuple()),
               'standard_label': Text.snakify(edge.standard_predicate.label),
               'standard_id': edge.standard_predicate.identifier,
               'original_predicate_id': edge.original_predicate.identifier,
               'original_predicate_label': edge.original_predicate.label,
               'publication_count': len(edge.publications),
               'publications': edge.publications[:1000],
               'url' : edge.url,
               'input': edge.input_id
               }
              for edge in edgelist]

    tx.run(cypher,{'batches': batch})

    for edge in edgelist:
        if edge.standard_predicate.identifier == 'GAMMA:0':
            logger.warn(f"Unable to map predicate for edge {edge.original_predicate}  {edge}")

def sort_nodes_by_label(nodes):
    nl = defaultdict(list)
    deque( map( lambda x: nl[x.node_type].append(x), nodes ) )
    return nl


def export_node_chunk(tx,nodelist,label):
    cypher = """UNWIND {batches} as batch
                MERGE (a:%s {id: batch.id})
                set a:%s
                set a.name=batch.label
                set a.equivalent_identifiers=batch.syn
                """ % (node_types.ROOT_ENTITY, label)
    propnames = set()
    for node in nodelist:
        propnames.update(node.properties.keys())
    for pname in propnames:
        cypher += f'set a.{pname}=batch.{pname}'
    batch = []
    for i,n in enumerate(nodelist):
        nodeout = { 'id': n.identifier, 'label': n.label, 'syn': [s.identifier for s in n.synonyms] }
        for pname in propnames:
            nodeout[pname] = n.properties[pname]
        batch.append(nodeout)
    tx.run(cypher,{'batches': batch})


