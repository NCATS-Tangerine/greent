import json
import unittest
from pprint import pprint
from greent.neo4jbase import Neo4JREST
from greent.util import Munge
from greent.util import Text
from greent.graph_components import KEdge, KNode
from greent import node_types
from datetime import datetime as dt

"""
Notes: 
 Hetio contains:
  anatomy terms that use an UBERON curie as the "identifier" property
  genes that use the Entrez ID (NCBIGENE) number (not curie and not string) as the "identifier" property
  diseases use a DOID curie as the "identifier" property
  phenotypes are in there as symptoms.  The identifier property is a Mesh ID (not a curie)
"""

class HetIO(Neo4JREST):

    def __init__(self, context): #url="https://neo4j.het.io"):
        super (HetIO, self).__init__("hetio", context)

    def munge_gene (self, gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene

    #TODO: also make an anatomy to gene, check directions
    def gene_to_anatomy (self, gene):
        gene_identifiers = list(gene.get_synonyms_by_prefix('NCBIGENE'))
        gene_identifier = Text.un_curie(gene_identifiers[0])
        nodes,edges = self.query ( "MATCH (a:Anatomy)-[ar]-(g:Gene) WHERE g.identifier={0} RETURN a, ar, g LIMIT 200".format (gene_identifier),
                              labels=['Anatomy'], kinds=['node','relationship'])
        node_ids = [ node['identifier'] for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate_id = f'hetio:{predicate_label}'
            standard_predicate_id, standard_predicate_label = self.standardize_predicate(predicate_id, predicate_label)
            results.append( (KEdge('hetio.gene_to_anatomy',dt.now(),predicate_id,predicate_label,gene_identifier,
                             standard_predicate_id, standard_predicate_label),
                             KNode(node_id, node_types.ANATOMY) ) )
        return results

    #TODO: this is not to a cell, but a cellular component.  REmoving it from the yaml until we can fix it up
    def gene_to_cellular_component (self, gene):
        result = self.query (
            "MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='{0}' RETURN g, r, c LIMIT 200".format (Text.un_curie (gene.identifier)),
            labels=['CellularComponent'],
            node_properties=['identifier'])
        anatomies = []
        return [ ( self.get_edge ({ 'res' : r }, predicate='affects'), KNode(r['identifier'], node_types.CELLULAR_COMPONENT) ) for r in result ]

    #TODO: implement the reverse too
    def gene_to_disease (self, gene):
        gene_identifiers = list(gene.get_synonyms_by_prefix('NCBIGENE'))
        gene_identifier = Text.un_curie(gene_identifiers[0])
        nodes, edges = self.query (
            "MATCH (d:Disease)-[a1]-(g:Gene) WHERE g.identifier={0} RETURN a1,d".format (gene_identifier),
            labels=['Disease'], kinds=['node','relationship'])
        node_ids = [ node['identifier'] for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate_id = f'hetio:{predicate_label}'
            standard_predicate_id, standard_predicate_label = self.standardize_predicate(predicate_id, predicate_label)
            results.append( (KEdge('hetio.gene_to_disease',dt.now(),predicate_id,predicate_label,gene_identifier,
                             standard_predicate_id, standard_predicate_label),
                             KNode(node_id, node_types.DISEASE) ) )
        return results

    def disease_to_phenotype (self, disease):
        disease_identifiers = list(disease.get_synonyms_by_prefix('DOID'))
        disease_identifier = disease_identifiers[0]
        query = """MATCH (d:Disease{identifier:'%s'})-[r]-(s:Symptom) RETURN d,r,s""" % (disease_identifier)
        nodes,edges = self.query (query, labels=['Symptom'], kinds=['node','relationship'])
        node_ids = [ f"MESH:{node['identifier']}" for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate_id = f'hetio:{predicate_label}'
            standard_predicate_id, standard_predicate_label = self.standardize_predicate(predicate_id, predicate_label)
            results.append( (KEdge('hetio.disease_to_phenotype',dt.now(),predicate_id,predicate_label,disease_identifier,
                             standard_predicate_id, standard_predicate_label),
                             KNode(node_id, node_types.PHENOTYPE) ) )
        return results

'''
class TestHetIO(unittest.TestCase):

    h = HetIO (ServiceContext.create_context ())
    
    def test_anatomy (self):
        pprint (self.h.gene_to_anatomy (KNode('HGNC:TP53', node_types.GENE)))

    def test_cell (self):
        pprint (self.h.gene_to_cell (KNode('HGNC:7121', node_types.GENE)))

if __name__ == '__main__':
    
    het = HetIO (ServiceContext.create_context ())
    print (het.disease_to_phenotype (KNode('DOID:2841',node_types.DISEASE)))
'''
#MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='HGNC:3263' RETURN g, r, c LIMIT 200
