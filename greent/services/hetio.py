import json
import unittest
from pprint import pprint
from greent.neo4jbase import Neo4JREST
from greent.util import Munge
from greent.util import Text
from greent.graph_components import KEdge, KNode
from greent import node_types 
from greent.service import ServiceContext

class HetIO(Neo4JREST):

    def __init__(self, context): #url="https://neo4j.het.io"):
        super (HetIO, self).__init__("hetio", context)

    def munge_gene (self, gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene

    def gene_to_anatomy (self, gene):
        result = self.query (
            "MATCH (a:Anatomy)-[ar]-(g:Gene) WHERE g.name='{0}' RETURN a, ar, g LIMIT 200".format (Text.un_curie (gene.identifier)),
            labels=['Anatomy'],
            node_properties=['identifier'])
        print (result)
        return [ ( self.get_edge ({ 'res', r }, predicate='involved_in'), KNode(r['identifier'],  node_types.ANATOMY) ) for r in result ]
    
    def gene_to_cell (self, gene):
        result = self.query (
            "MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='{0}' RETURN g, r, c LIMIT 200".format (Text.un_curie (gene.identifier)),
            labels=['CellularComponent'],
            node_properties=['identifier'])
        return [ ( self.get_edge ({ 'res' : r }, predicate='affects'), KNode(r['identifier'], node_types.CELLULAR_COMPONENT) ) for r in result ]

    def gene_to_disease (self, gene):
        if not Text.get_curie(gene.identifier) in [ 'HGNC', 'UNIPROT', 'PHAROS' ]:
            return []
        result = self.query (
            "MATCH (d:Disease)-[a1]-(g:Gene) WHERE g.name='{0}' RETURN a1,d".format (Text.un_curie(gene.identifier)),
            labels=['Disease'])
        #        result = self.nodes_and_edges (result)
        for r in result:
            print (r)
            print (result)
            print (type(result))
        #print ("-------------------> {}".format (json.dumps (result, indent=2)))
        return [ ( self.get_edge ({ 'res' : r }, predicate='affects'), KNode(r['identifier'], node_types.DISEASE) ) for r in result ]
    
    def disease_to_phenotype (self, disease):
        query = """MATCH (d:Disease{identifier:'%s'})-[r]-(s:Symptom) RETURN d,r,s""" % (disease.identifier)
        result = self.query (query, labels=['Symptom'], node_properties=None)
        edge_node = []
        for r in result:
            if r['source'] == 'MeSH':
                edge_node.append ( ( self.get_edge ({ 'res' : r }, predicate='affects'), KNode("MESH:{0}".format (r['identifier']), node_types.PHENOTYPE) ) )
        return edge_node
    
        #return [ ( self.get_edge ({ 'res' : r }, predicate='affects'), KNode("MESH:{0}".format (r['identifier']), 'PH') ) for r in result ]
        
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
    with open('hgnc-entrez', 'r') as stream:
        for line in stream:
            h, e, u = line.split ('\t')
            het.gene_to_anatomy (KNode('SOMETHING:{}'.format (e), node_types.GENE))
    '''
    #unittest.main ()


#MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='HGNC:3263' RETURN g, r, c LIMIT 200
