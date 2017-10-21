import json
import unittest
from pprint import pprint
from greent.neo4j import Neo4JREST
from greent.util import Munge
from reasoner.graph_components import KEdge, KNode

class HetIO(Neo4JREST):

    def __init__(self, url="https://neo4j.het.io"):
        super (HetIO, self).__init__(url)

    def munge_gene (self, gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene

    def gene_to_anatomy (self, gene):
        result = self.query (
            "MATCH (a:Anatomy)-[ar]-(g:Gene) WHERE g.name='%s' RETURN a, ar, g LIMIT 5" % gene.identifier.split (":")[1],
            labels=['Anatomy'],
            node_properties=['identifier'])
        return [ ( KEdge('gene-anat', 'queried'), KNode(r['identifier'], 'A') ) for r in result ]
    
    def gene_to_cell (self, gene):
        result = self.query (
            "MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='%s' RETURN g, r, c LIMIT 200" %  gene.identifier.split (":")[1],
            labels=['CellularComponent'],
            node_properties=['identifier'])
        return [ ( KEdge('gene-cell', 'queried'), KNode(r['identifier'], 'C') ) for r in result ]
    
class TestHetIO(unittest.TestCase):

    h = HetIO ()
    
    def test_anatomy (self):
        pprint (self.h.gene_to_anatomy ('TP53'))

    def test_cell (self):
        pprint (self.h.gene_to_cell ('TP53'))
    
if __name__ == '__main__':
    unittest.main ()

