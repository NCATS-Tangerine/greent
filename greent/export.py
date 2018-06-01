from greent.graph_components import KNode, KEdge
from greent import node_types
from greent.util import LoggingUtil,Text
from greent.rosetta import Rosetta
from neo4j.v1 import GraphDatabase
from collections import defaultdict, deque
import calendar
import logging

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

def export_graph(graph, rosetta):
    """Export to neo4j database."""
    # TODO: lots of this should probably go in the KNode and KEdge objects?
    logger.info("Writing graph to neo4j")
    # Now add all the nodes
    export_nodes(graph.nodes(),rosetta)
    export_edges(graph.edges(data=True),rosetta)
    logger.info(f"Wrote {len(graph.nodes())} nodes and {len(graph.edges())} edges.")

def _get_driver(rosetta):
    config = rosetta.type_graph.get_config()
    driver = GraphDatabase.driver(config['url'], auth=("neo4j", config['neo4j_password']))
    return driver

def export_edges(edges,rosetta):
    driver = _get_driver(rosetta):
    edges_by_label = sort_edges_by_label(edges)
    for label,nodelist in edges_by_label.items():
        chunksize = 1000
        for chunknum in range(0, len(nodelist), chunksize):
            chunk = nodelist[chunknum:chunknum+chunksize]
            with driver.session() as session:
                session.write_transaction(export_edge_chunk,chunk,label)

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
    batch = [ {'aid': edge[0].identifier,
               'bid': edge[1].identifier,
               'edge_source': edge[2]['object'].edge_source,
               'database': edge[2]['object'].edge_source.split('.')[0],
               'ctime': calendar.timegm(edge[2]['object'].ctime.timetuple()),
               'standard_label': Text.snakify(edge[2]['object'].standard_predicate.label),
               'standard_id': edge[2]['object'].standard_predicate.identifier,
               'original_predicate_id': edge[2]['object'].original_predicate.identifier,
               'original_predicate_label': edge[2]['object'].original_predicate.label,
               'publication_count': len(edge[2]['object'].publications),
               'publications': edge[2]['object'].publications[:1000],
               'url' : edge[2]['object'].url,
               'input': edge[2]['object'].input_id
               }
              for edge in edgelist]
    tx.run(cypher,{'batches': batch})

    for edge in edgelist:
        ke = edge[2]['object']
        if ke.standard_predicate.identifier == 'GAMMA:0':
            logger.warn(f"Unable to map predicate for edge {ke.original_predicate}  {ke}")

def sort_nodes_by_label(nodes):
    nl = defaultdict(list)
    deque( map( lambda x: nl[x.node_type].append(x), nodes ) )
    return nl

def export_nodes(nodes,rosetta):
    driver = _get_driver(rosetta)
    nodes_by_label = sort_nodes_by_label(nodes)
    for label,nodelist in nodes_by_label.items():
        chunksize = 1000
        for chunknum in range(0, len(nodelist), chunksize):
            chunk = nodelist[chunknum:chunknum+chunksize]
            with driver.session() as session:
                session.write_transaction(export_node_chunk,chunk,label)

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


# TODO: push to node, ...
def prepare_node_for_output(node, gt):
    logger.debug('Prepare: {} {}'.format(node.identifier, node.label))
    #logger.debug('  Synonyms: {}'.format(' '.join(list(node.synonyms))))
    node.synonyms.update([mi['curie'] for mi in node.mesh_identifiers if mi['curie'] != ''])
    if node.node_type == node_types.DISEASE or node.node_type == node_types.GENETIC_CONDITION:
        if 'mondo_identifiers' in node.properties:
            node.synonyms.update(node.properties['mondo_identifiers'])
        try:
            node.label = gt.mondo.get_label(node.identifier)
        except:
            if node.label is None:
                node.label = node.identifier
    if node.label is None:
        if node.node_type == node_types.GENE and node.identifier.startswith('HGNC:'):
            node.label = gt.hgnc.get_name(node)
        elif node.node_type == node_types.GENE and node.identifier.upper().startswith('NCBIGENE:'):
            node.label = gt.hgnc.get_name(node)
        elif node.node_type == node_types.CELL and node.identifier.upper().startswith('CL:'):
            try:
                node.label = gt.uberongraph.cell_get_cellname(node.identifier)[0]['cellLabel']
            except:
                logger.error('Error getting cell label for {}'.format(node.identifier))
                node.label = node.identifier
        else:
            node.label = node.identifier
    logger.debug('Prepared: {} {}'.format(node.identifier, node.label))


