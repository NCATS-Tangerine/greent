import logging
from greent.triplestore import TripleStore
from greent.util import LoggingUtil
import unittest
from pprint import pprint

logger = LoggingUtil.init_logging (__file__)

class MeSH(object):
    def __init__(self, uri="http://id.nlm.nih.gov/mesh/sparql"):
        self.triplestore = TripleStore (uri)
    def get_broader (self, term):
        return self.triplestore.query_template (
            inputs={ "term" : term, "prefixes" : self.get_prefixes () },
            outputs= [ "obj", "name" ],
            template_text="""
            $prefixes
            SELECT DISTINCT ?obj ?name ?itemName FROM  <http://id.nlm.nih.gov/mesh>
            WHERE {
               ?item  meshv:broaderDescriptor ?obj ;
                      rdfs:label              ?itemName.
               ?obj   rdfs:label              ?name .
              filter (regex(lcase(str(?itemName)), lcase(str("$term"))))
            } 
            ORDER BY ?p
            """)

    """
    SELECT DISTINCT ?obj ?name FROM  <http://id.nlm.nih.gov/mesh>
            WHERE {
               $term   meshv:broaderDescriptor ?obj .
               ?obj    rdfs:label              ?name .
            } 
            ORDER BY ?p
            """
    def get_prefixes (self):
        return """
        PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>
        PREFIX owl:      <http://www.w3.org/2002/07/owl#>
        PREFIX meshv:    <http://id.nlm.nih.gov/mesh/vocab#>
        PREFIX mesh:     <http://id.nlm.nih.gov/mesh/>
        PREFIX mesh2015: <http://id.nlm.nih.gov/mesh/2015/>
        PREFIX mesh2016: <http://id.nlm.nih.gov/mesh/2016/>
        PREFIX mesh2017: <http://id.nlm.nih.gov/mesh/2017/>"""

    
class TestMeSH(unittest.TestCase):

    m = MeSH ()
    def test_get_broader (self):
        pprint (self.m.get_broader ("Asthma"))  #"mesh:D001249"))

if __name__ == '__main__':
    unittest.main ()

