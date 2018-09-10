import requests
import pprint
from greent.service import Service
from greent.util import Text,LoggingUtil
from greent import node_types
import logging
import time

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class UniChem(Service):

    def __init__(self, context):
        super(UniChem, self).__init__("unichem", context)
        #Sources in UniChem are listed by a number
        # 1 = Chembl
        # 2 = PubChem
        # 7 = CHEBI
        self.curie_to_sourceid = { 'CHEMBL': '1', 'DRUGBANK': '2', 'CHEBI': '7' , 'PUBCHEM': '22'}
        self.sourceid_to_curie = { v:k for k,v in self.curie_to_sourceid.items()}

    #TODO: share the retry logic in Service?
    def query(self,url):
        done = False
        num_tries = 0
        max_tries = 10
        wait_time = 5 # seconds
        while num_tries < max_tries:
            try:
                return requests.get(url).json()
            except:
                num_tries += 1
                time.sleep(wait_time)
        return None
 
    # Identifiers going into and coming out from the service are not curies, just identifiers
    def get_synonyms(self, identifier):
        logger.debug('Hi, Unichem')
        curie = Text.get_curie(identifier)
        if curie not in self.curie_to_sourceid:
            return set()
        bare_id = Text.un_curie(identifier)
        url = "{0}/src_compound_id/{1}/{2}".format(self.url, bare_id, self.curie_to_sourceid[curie] )
        logger.debug(url)
        #response = requests.get(url).json ()
        response = self.query(url)
        if response is None:
            logger.error(f'UniChem bombed on {identifier}')
            return set()
        if 'error' in response:
            return set()
        results = set()
        for result in response:
            sid = result['src_id']
            if sid in self.sourceid_to_curie:
                curie = self.sourceid_to_curie[sid]
                results.add(f'{curie}:{result["src_compound_id"]}')
        return results



