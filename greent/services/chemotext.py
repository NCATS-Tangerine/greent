import json
import os
import unittest
from collections import defaultdict

from ontobio.vocabulary import upper

from greent.mesh import MeSH
from greent.neo4jbase import Neo4JREST
from greent.util import LoggingUtil
from pprint import pprint
from greent.graph_components import KNode, KEdge
from greent import node_types

logger = LoggingUtil.init_logging (__file__)

class Chemotext(Neo4JREST):
    def __init__(self, context): #url="http://chemotext.mml.unc.edu:7474"):
        super (Chemotext, self).__init__("chemotext", context)
        self.mesh = MeSH ()
        self.cache = os.path.join(os.path.dirname(__file__),'chemotext.words.txt')
        self.mesh_map_name = os.path.join(os.path.dirname(__file__),'meshnames_2018.bin')
        if not os.path.exists(self.cache):
            build_synonym_cache()
        if not os.path.exists(self.mesh_map_name):
            parse_mesh_files()
        self.load_synonym_cache()

    def load_synonym_cache( self ):
        self.term_map = {}
        with open(self.cache,'r') as infile:
            h = infile.readline()
            for line in infile:
                x = line.strip().split('\t')
                self.term_map[x[0]] = x[1]
        self.mesh_id_map = {}
        with open(self.mesh_map_name,'r') as infile:
            h = infile.readline()
            for line in infile:
                x = line.strip().split('\t')
                self.mesh_id_map[x[0]] = x[1]
        
    def term_to_term (self, A, of_type=None, limit=100):
        response = self.query (
            query="MATCH (d:Term)-[r1]-(a:Art)-[r2]-(t:Term) WHERE d.name='%s' RETURN d, r1, a, r2, t LIMIT %s" % (A, limit))
        response = self.filter_nodes (response, labels=['Term'], properties=['name', 'type'])

        # Use MeSH data - slow but richer
        if of_type != None and response != None:
            new_response = []
            groups = defaultdict (lambda:None)
            for r in response:
                groups[r['name']] = r                
            for thing in groups:
                #logger.debug (" --mesh-req: {0}".format (thing))
                broader = self.mesh.get_broader (thing)
                obj = groups[thing]
                for k, v in of_type.items ():
                    for b in broader:
                        if k in b and b[k] == v:
                            #print ("b k v {0} {1} {2}".format (b, k, v))
                            obj['category'] = b['name']
                            obj['id'] = b['obj']
                            new_response.append (obj)
            response = new_response
        return response
    
    def disease_name_to_drug_name (self, disease, limit=100):
        result = []
        response = self.query (
            query="MATCH (d:Term {type:'Disease', name: '%s' })-[r1]-(a:Art)-[r2]-(t:Term {isDrug:true}) RETURN d, r1, a, r2, t LIMIT %s" %
            (disease, limit))
        for r in response['results'][0]['data']:
            result.append (r['row'][4]['name'])
        return list(set(list(result)))

    def graph_disease_name_to_drug_name (self, disease, limit=100):
        result = []
        drug_names = self.disease_name_to_drug_name (disease, limit)
        for r in drug_names:
            result.append ( ( self.get_edge (props=r), KNode("DRUGBANK.NAME:{0}".format (r['name']), node_types.NAME_DRUG) ) )
        return result

    def get_chemotext_term(self, input_term):
        qterm = input_term.upper()
        if qterm not in self.term_map:
            return None
        return self.term_map[qterm]

    def get_chemotext_term_from_meshid(self,mesh_id):
        if mesh_id not in self.mesh_id_map:
            return None
        return self.get_chemotext_term(self.mesh_id_map[mesh_id])
    

def build_synonym_cache(ctext = None):
    if ctext is None:
        from greent.service import ServiceContext
        ctext = Chemotext(ServiceContext.create_context())
    response = ctext.query( query="MATCH (d:Term) RETURN d")
    with open(ctext.cache,'w') as outfile:
        outfile.write('QUERY\tKEY\n')
        res = response['results'][0]
        n = 0
        for datum in res['data']:
            rows = datum['row']
            for row in rows:
                n+=1
                rowtype = row['type']
                meshname = row['name']
                if 'synonyms' in row:
                    rowsyn  = row['synonyms']
                else:
                    rowsyn = []
                outfile.write('{}\t{}\n'.format(meshname.upper(), meshname))
                for syn in rowsyn:
                    outfile.write('{}\t{}\n'.format(syn.upper(), meshname) )


def parse_mesh_files():
    mesh_map_name = os.path.join(os.path.dirname(__file__),'meshnames_2018.bin')
    with open(mesh_map_name,'w') as outfile:
        for branch,namekey in (('c','NM'),('d','MH'),('q','SH')):
            mesh_file_name = os.path.join(os.path.dirname(__file__),f'{branch}2018.bin')
            with open(mesh_file_name) as infile:
                line = infile.readline()
                while line != '':
                    #Following loops over a *NEWRECORD
                    while len(line.strip())>0:
                        if line.startswith('*NEWRECORD'):
                            line = infile.readline()
                            continue
                        kv = line.strip().split(' = ')
                        if kv[0] == namekey:
                            name = kv[1]
                        if kv[0] == 'UI':
                            meshid = kv[1]
                        line = infile.readline()
                    outfile.write(f'{meshid}\t{name}\n')
                    line = infile.readline()


'''
class TestChemotext(unittest.TestCase):

    chemotext = Chemotext (GreenT())
    
    def test_disease_to_drug (self):
        pprint ("Disease name to drug name:")
        pprint (self.chemotext.disease_name_to_drug_name ("Asthma"))

    def test_term_to_term (self):
        pprint (self.chemotext.term_to_term ('Asthma')[:10])

    def test_term_to_term_of_type (self):
        pprint ("term to term of type")
        pprint (self.chemotext.term_to_term ('Asthma', of_type={'name': 'Respiratory Hypersensitivity' }))
'''
if __name__ == '__main__':
    #pass
    #build_synonym_cache()
    parse_mesh_files()
    #unittest.main ()

