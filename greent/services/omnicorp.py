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
        self.prefix_to_uri = {'UBERON': 'http://purl.obolibrary.org/obo/UBERON_',
                              'BSPO': 'http://purl.obolibrary.org/obo/BSPO_',
                              'PATO': 'http://purl.obolibrary.org/obo/PATO_',
                              'GO':'http://purl.obolibrary.org/obo/GO_',
                              'MONDO':'http://purl.obolibrary.org/obo/MONDO_',
                              'HP':'http://purl.obolibrary.org/obo/HP_',
                              'ENVO:':'http://purl.obolibrary.org/obo/ENVO_',
                              'OBI':'http://purl.obolibrary.org/obo/OBI_',
                              'CL':'http://purl.obolibrary.org/obo/CL_',
                              'SO':'http://purl.obolibrary.org/obo/SO_',
                              'CHEBI':'http://purl.obolibrary.org/obo/CHEBI_',
                              'HGNC':'http://identifiers.org/hgnc/HGNC:',
                              'MESH':'http://id.nlm.nih.gov/mesh/'}

    def get_omni_identifier(self,node):
        #Let's start with just the 'best' identifier
        identifier = node.identifier
        prefix = Text.get_curie(node.identifier)
        if prefix not in self.prefix_to_uri:
            logger.error("What kinda tomfoolery is this?")
            logger.error(f"{node.identifier} {node.node_type}")
            logger.error(f"{node.synonyms}")
        oident = f'{self.prefix_to_uri[prefix]}{Text.un_curie(node.identifier)}'
        return oident

    def query_omnicorp (self, query):
        """ Execute and return the result of a SPARQL query. """
        return self.triplestore.execute_query (query)

    def sparql_get_shared_pmids (self, identifier_a, identifier_b):
        text = """
        PREFIX dct: <http://purl.org/dc/terms/>
        SELECT DISTINCT ?pubmed
        WHERE {
          ?pubmed dct:references <$id_a> .
          ?pubmed dct:references <$id_b> .
        }
        """
        logger.debug(text)
        results = self.triplestore.query_template( 
            inputs = { 'id_a': identifier_a, 'id_b': identifier_b }, \
            outputs = [ 'pubmed' ], \
            template_text = text \
        )
        return results

    def get_shared_pmids (self, node1, node2):
        id1 = self.get_omni_identifier(node1)
        id2 = self.get_omni_identifier(node2)
        pmids = self.sparql_get_shared_pmids (id1, id2)
        return [ p['pubmed'] for p in pmids ]
    


