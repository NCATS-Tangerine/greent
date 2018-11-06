from string import Template
import json
import os
import logging
from greent.service import Service
from greent.triplestore import TripleStore
from greent.util import LoggingUtil
from greent.util import Text
from greent import node_types
from pprint import pprint
import datetime
from collections import defaultdict
import time
import psycopg2

logger = LoggingUtil.init_logging(__name__, logging.INFO)

"""OMNICORP IS NO LONGER CALLED FROM INTERFACES"""

class OmniCorp(Service):

    def __init__(self, context): #triplestore):
        super(OmniCorp, self).__init__("omnicorp", context)
        db = context.config['OMNICORP_DB']
        user = context.config['OMNICORP_USER']
        port = context.config['OMNICORP_PORT']
        host = context.config['OMNICORP_HOST']
        password = context.config['OMNICORP_PASSWORD']
        self.prefixes = set(['UBERON', 'BSPO', 'PATO', 'GO', 'MONDO', 'HP', 'ENVO', 'OBI', 'CL', 'SO', 'CHEBI', 'HGNC', 'MESH'])
        #self.conn = psycopg2.connect(dbname=db, user=user, host=host, port=port, password=password)
	self.conn = None
        self.nsingle = 0
        self.total_single_call = datetime.timedelta()
        self.npair = 0
        self.total_pair_call = datetime.timedelta()

    def __del__(self):
        self.conn.close()

    def get_omni_identifier(self,node):
        #Let's start with just the 'best' identifier
        identifier = node.id
        prefix = Text.get_curie(node.id)
        if prefix not in self.prefixes:
            #logger.debug("What kinda tomfoolery is this?")
            #logger.debug(f"{node.id} {node.type}")
            #logger.debug(f"{node.synonyms}")
            return None
        return identifier

    def get_shared_pmids (self, node1, node2):
        id1 = self.get_omni_identifier(node1)
        id2 = self.get_omni_identifier(node2)
        if id1 is None or id2 is None:
            return []
        done = False
        ntries = 0
        pmids = self.postgres_get_shared_pmids( id1,id2 )
        if pmids is None:
            logger.error("OmniCorp gave up")
            return None
        return [ f'PMID:{p}' for p in pmids ]

    def postgres_get_shared_pmids(self, id1, id2):
        prefix1 = Text.get_curie(id1)
        prefix2 = Text.get_curie(id2)
        start = datetime.datetime.now()
        cur = self.conn.cursor()
        statement = f'''SELECT a.pubmedid 
           FROM omnicorp.{prefix1} a
           JOIN omnicorp.{prefix2} b ON a.pubmedid = b.pubmedid
           WHERE a.curie = %s
           AND b.curie = %s '''
        cur.execute(statement, (id1, id2))
        pmids = [ x[0] for x in cur.fetchall() ]
        cur.close()
        end = datetime.datetime.now()
        self.total_pair_call += (end-start)
        logger.debug(f'Found {len(pmids)} shared ids in {end-start}. Total {self.total_pair_call}')
        self.npair += 1
        if self.npair % 100 == 0:
            logger.info(f'NCalls: {self.npair} Total time: {self.total_pair_call}  Avg Time: {self.total_pair_call/self.npair}')
        return pmids

    def count_pmids(self, node):
        identifier = self.get_omni_identifier(node)
        if identifier is None:
            return 0
        prefix = Text.get_curie(identifier)
        start = datetime.datetime.now()
        cur = self.conn.cursor()
        statement = f'SELECT COUNT(pubmedid) from omnicorp.{prefix} WHERE curie = %s'
        cur.execute(statement, (identifier,))
        n = cur.fetchall()[0][0]
        cur.close()
        end = datetime.datetime.now()
        self.total_single_call += (end-start)
        logger.debug(f'Found {n} pmids in {end-start}. Total {self.total_single_call}')
        self.nsingle += 1
        if self.nsingle % 100 == 0:
            logger.info(f'NCalls: {self.nsingle} Total time: {self.total_single_call}  Avg Time: {self.total_single_call/self.nsingle}')
        return n

'''
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
    

'''
