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
from collections import defaultdict
import time

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

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
        identifier = node.id
        prefix = Text.get_curie(node.id)
        if prefix not in self.prefix_to_uri:
            logger.warn("What kinda tomfoolery is this?")
            logger.warn(f"{node.id} {node.type}")
            logger.warn(f"{node.synonyms}")
            return None
        oident = f'{self.prefix_to_uri[prefix]}{Text.un_curie(node.id)}'
        return oident

    def query_omnicorp (self, query):
        """ Execute and return the result of a SPARQL query. """
        return self.triplestore.execute_query (query)

    def sparql_get_all_shared_pmids (self, identifier_list):
        text = """
        PREFIX dct: <http://purl.org/dc/terms/>
        SELECT DISTINCT ?pubmed ?term1 ?term2
        WHERE {
          hint:Query hint:analytic true .
          VALUES ?term1 $id_list_a
          VALUES ?term2 $id_list_b
          ?pubmed dct:references ?term1 .
          ?pubmed dct:references ?term2 .
          FILTER(STR(?term1) < STR(?term2))
        }
        """
        start = datetime.datetime.now()
        results = self.triplestore.query_template(
                inputs = { 'id_list_a': identifier_list, 'id_list_b': identifier_list },
            outputs = [ 'term1','term2','pubmed' ],
            template_text = text,
            post = True
        )
        end = datetime.datetime.now()
        logger.debug(f'Completed in: {end-start}')
        return results

    def sparql_count_pmids (self, identifier):
        text = """
        PREFIX dct: <http://purl.org/dc/terms/>
        SELECT (COUNT(DISTINCT ?pubmed) as ?count) 
        WHERE {
          hint:Query hint:analytic true .
          ?pubmed dct:references <$identifier> .
        }
        """
        results = self.triplestore.query_template(
            inputs = { 'identifier': identifier },
            outputs = [ 'count' ],
            template_text = text,
        )
        return results

    def sparql_get_shared_pmids (self, identifier_a, identifier_b):
        text = """
        PREFIX dct: <http://purl.org/dc/terms/>
        SELECT DISTINCT ?pubmed
        WHERE {
          hint:Query hint:analytic true .
          ?pubmed dct:references <$id_a> .
          ?pubmed dct:references <$id_b> .
        }
        """
        results = self.triplestore.query_template(
            inputs = { 'id_a': identifier_a, 'id_b': identifier_b },
            outputs = [ 'pubmed' ],
            template_text = text,
            post = True
        )
        return results

    def get_all_shared_pmids (self, nodes):
        oiddict = {self.get_omni_identifier(n):n for n in nodes}
        oids = [f'<{x}>' for x in filter(lambda n: n is not None, oiddict.keys())]
        oidsstring = '{ ' + ' '.join(oids) + '}'
        results = self.sparql_get_all_shared_pmids(oidsstring)
        pubmeds = defaultdict( list )
        for r in results:
            k = (oiddict[r['term1']],oiddict[r['term2']])
            pubmeds[k].append(f"PMID:{r['pubmed'].split('/')[-1]}")
        for i,node_i in enumerate(nodes):
            for node_j in nodes[:i]:
                k_ij = (node_i, node_j)
                k_ji = (node_j, node_i)
                if k_ij not in pubmeds and k_ji not in pubmeds:
                    pubmeds[k_ij] = []
        return pubmeds

    def call_with_retries(self,fnc,args):
        done = False
        ntries = 0
        maxtries = 100
        rest_time = 10 #seconds
        start = datetime.datetime.now()
        while not done and ntries < maxtries:
            try:
                result = fnc(*args)
                done = True
            except:
                logger.warn("OmniCorp error, retrying")
                time.sleep(rest_time)
                ntries += 1
        if not done:
            return None
        else:
            end = datetime.datetime.now()
            logger.debug(f'Total call ntries: {ntries}, time: {end-start}')
            return result

    def count_pmids(self, node):
        identifier = self.get_omni_identifier(node)
        if identifier is None:
            return 0
        res = self.call_with_retries(self.sparql_count_pmids, [identifier])
        if res is None:
            return None
        else:
            logger.debug(f"Returned {res[0]['count']}")
            return res[0]['count']

    def get_shared_pmids (self, node1, node2):
        id1 = self.get_omni_identifier(node1)
        id2 = self.get_omni_identifier(node2)
        if id1 is None or id2 is None:
            return []
        done = False
        ntries = 0
        pmids = self.call_with_retries(self.sparql_get_shared_pmids, [id1,id2])
        if pmids is None:
            logger.error("OmniCorp gave up")
            return None
        return [ p['pubmed'] for p in pmids ]
    


