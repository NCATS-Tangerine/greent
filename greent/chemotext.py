import json
import unittest
from collections import defaultdict
from greent.mesh import MeSH
from greent.neo4j import Neo4JREST
from pprint import pprint
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging (__file__)

class Chemotext(Neo4JREST):
    def __init__(self, url="http://chemotext.mml.unc.edu:7474"):
        super (Chemotext, self).__init__(url)
        self.mesh = MeSH ()
        
    def term_to_term (self, A, of_type=None, limit=100):
        response = self.query (
            query="MATCH (d:Term)-[r1]-(a:Art)-[r2]-(t:Term) WHERE d.name='%s' RETURN d, r1, a, r2, t LIMIT %s" % (A, limit))
        response = self.filter_nodes (response, labels=['Term'], properties=['name', 'type'])        

        ''' use chemotext types (coarse grained but fast)
        if of_type != None and response != None:
            new_response = []
            for r in response:
                for k, v in of_type.items ():
                    print (" {0} {1} {2}".format (k, v, r))
                    if k in r and r[k] == v:
                        new_response.append (r)
            response = r
            #response = [ r for t in of_type for r in response if r['type'] == t ]
        '''

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
        #print (json.dumps (response, indent=2))
        for r in response['results'][0]['data']:
            result.append (r['row'][4]['name'])
        return list(set(list(result)))

class TestChemotext(unittest.TestCase):

    chemotext = Chemotext ()
    
    def test_disease_to_drug (self):
        pprint ("Disease name to drug name:")
        pprint (self.chemotext.disease_name_to_drug_name ("Asthma"))

    def test_term_to_term (self):
        pprint (self.chemotext.term_to_term ('Asthma')[:10])

    def test_term_to_term_of_type (self):
        pprint ("term to term of type")
        pprint (self.chemotext.term_to_term ('Asthma', of_type={'name': 'Respiratory Hypersensitivity' }))

if __name__ == '__main__':
    unittest.main ()

