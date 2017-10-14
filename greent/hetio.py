import json
import unittest
from pprint import pprint
from greent.neo4j import Neo4JREST
from greent.util import Munge

class HetIO(Neo4JREST):

    def __init__(self, url="https://neo4j.het.io"):
        super (HetIO, self).__init__(url)

    def munge_gene (self, gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene

    def gene_to_anatomy (self, gene):
        return self.query (
            "MATCH (a:Anatomy)-[ar]-(g:Gene) WHERE g.name='%s' RETURN a, ar, g LIMIT 5" % Munge.gene (gene),
            labels=['Anatomy'],
            node_properties=['identifier'])

    def gene_to_cell (self, gene):
        gene = self.munge_gene (gene)
        return self.query (
            "MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='%s' RETURN g, r, c LIMIT 200" % Munge.gene (gene),
            labels=['CellularComponent'],
            node_properties=['identifier'])

class TestHetIO(unittest.TestCase):

    h = HetIO ()
    
    def test_anatomy (self):
        pprint (self.h.gene_to_anatomy ('TP53'))

    def test_cell (self):
        pprint (self.h.gene_to_cell ('TP53'))
    
if __name__ == '__main__':
    unittest.main ()

