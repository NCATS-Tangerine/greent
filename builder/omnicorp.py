import logging
from greent.graph_components import KEdge
from greent.util import Text
from greent.util import LoggingUtil
from greent import node_types
from collections import defaultdict
from datetime import datetime as dt

logger = LoggingUtil.init_logging (__file__, logging.DEBUG)

def get_supporter(greent):
    return OmnicorpSupport(greent)


class OmnicorpSupport():

    def __init__(self,greent):
        self.greent = greent
        self.omnicorp = greent.omnicorp

    def term_to_term(self,node_a,node_b):
        articles = self.omnicorp.get_shared_pmids(node_a, node_b)
        #logger.debug(f'OmniCorp {node_a.identifier} {node_b.identifier}')
        if len(articles) > 0:
            #logger.debug(f'    -> {len(articles)}')
            pmids = [f'PMID:{x.split("/")[-1]}' for x in articles]
            ke = KEdge('omnicorp.term_to_term', dt.now(), 'omnicorp:1', 'literature_co-occurence',
                       f'{node_a.identifier},{node_b.identifier}','omnicorp:1','literature_co-occurence',publications=pmids,
                       is_support=True)
            ke.source_node = node_a
            ke.target_node = node_b
            return ke
        return None


    def prepare(self,nodes):
        #no node prep required
        pass
