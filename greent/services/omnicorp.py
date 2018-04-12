from string import Template
import json
import os
import logging
from greent.service import Service
from greent.triplestore import TripleStore
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KEdge, KNode
from greent import node_types
from pprint import pprint
import datetime

logger = LoggingUtil.init_logging (__file__)

class OmniCorp(Service):

    def __init__(self, context): #triplestore):
        super(OmniCorp, self).__init__("omnicorp", context)
        self.triplestore = TripleStore (self.url)

    def query_omnicorp (self, query):
        """ Execute and return the result of a SPARQL query. """
        return self.triplestore.execute_query (query)


    def get_shared_pmids (self, node_a, node_b):
        """ Identify label for a cell type
        :param cell: CL identifier for cell type 
        """
        text = """
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        select distinct ?cellLabel
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
                  $cellID rdfs:label ?cellLabel .
              }
        """
        results = self.triplestore.query_template( 
            inputs = { 'cellID': cell_identifier }, \
            outputs = [ 'cellLabel' ], \
            template_text = text \
        )
        return results




    def get_shared_pmids (self, node1, node2):
        pmids = self.sparql_get_shared_pmids (node1, node2)
        #results = []
        #for r in anatomies:
        #    edge = KEdge ('uberongraph', 'cellToAnatomy')
        #    node = KNode (Text.obo_to_curie(r['anatomyID']), \
        #           node_types.ANATOMY )
        #    node.label = r['anatomyLabel']
        #    results.append ( (edge, node) )
        #return results
        return pmids
    


