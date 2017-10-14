import json
import os
import sys
import time
import unittest
from collections import defaultdict
from pprint import pformat
from pprint import pprint
from pprint import PrettyPrinter
from greent.client import GraphQL
from collections import namedtuple
from flask_testing import LiveServerTestCase

class TestTranslator(LiveServerTestCase):

    translator = GraphQL ("http://localhost:5001/graphql")    
    Translation = namedtuple ('Translation', [ 'thing', 'domain_a', 'domain_b', 'description' ])
    Translation.__new__.__defaults__ = (None, None, None, None, '')
    translations = [
        Translation ("Imatinib",     "http://chem2bio2rdf.org/drugbank/resource/Generic_Name", "http://chem2bio2rdf.org/uniprot/resource/gene",      "Drug->Target"),      
        Translation ("CDC25A",       "http://chem2bio2rdf.org/uniprot/resource/gene",          "http://chem2bio2rdf.org/kegg/resource/kegg_pathway", "Target->Pathway"), 
        Translation ("CACNA1A",      "http://chem2bio2rdf.org/uniprot/resource/gene",          "http://pharos.nih.gov/identifier/disease/name",      "Target->Disease"),     
        Translation ("Asthma",       "http://identifiers.org/mesh/disease/name",               "http://identifiers.org/mesh/drug/name",              "Disease->Drug"),              
        Translation ("DOID:2841",    "http://identifiers.org/doid",                            "http://identifiers.org/mesh/disease/id",             "DOID->MeSH"),             
        Translation ("MESH:D001249", "http://identifiers.org/mesh",                            "http://identifiers.org/doi",                         "MeSH->*")
    ]

    def create_app(self):
        from greent import app
        app = app.create_app(graphiql=True)
        app.config['TESTING'] = True
        app.config['LIVESERVER_PORT'] = 5001
        return app
    
    def test_translations (self):
        pp = PrettyPrinter (indent=4)
        for index, translation in enumerate (self.translations):
            name = "-- Translate {0} -> thing: {1} in domain {2} to domain {3}.".format (
                translation.description, translation.thing, translation.domain_a, translation.domain_b)
            with self.subTest (name=name):
                print ("{0} {1}".format (self.id (), name))
                pp.pprint (self.translator.translate (
                    thing=translation.thing,
                    domain_a=translation.domain_a,
                    domain_b=translation.domain_b))
                
    def run_test (self, translation):
        return self.translator.translate (thing=translation.thing, domain_a=translation.domain_a, domain_b=translation.domain_b)
    def find_test (self, name):
        result = None
        for t in self.translations:
            if t.description == name:
                result = t
                break
        return result
    def run_by_name (self, name):
        print ("Executing test: {0}".format (name))
        translation = self.find_test (name)
        print ("   -- Translate thing: {0} in domain {1} to domain {2}. {3}".format (
            translation.thing, translation.domain_a, translation.domain_b, translation.description))
        print (self.run_test (translation))
    '''
    def test_drug_to_target (self):
        time.sleep (3)
        self.run_by_name ("Drug->Target")
    def test_target_to_pathway (self):
        self.run_by_name ("Target->Pathway")
    def test_target_to_disease (self):
        self.run_by_name ("Target->Disease")
    def test_disease_to_drug (self):
        self.run_by_name ("Disease->Drug")
    def test_doid_to_mesh (self):
        self.run_by_name ("DOID->MeSH")
    def test_mesh_to_other (self):
        self.run_by_name ("MeSH->*")
    '''
    
if __name__ == '__main__':
    unittest.main ()
    sys.exit (0)
