import logging
from greent.graph_components import KEdge, LabeledID
from greent.util import Text
from greent.util import LoggingUtil
from greent import node_types
from collections import defaultdict
from datetime import datetime as dt

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

def get_supporter(greent):
    return OmnicorpSupport(greent)


class OmnicorpSupport():

    def __init__(self,greent):
        self.greent = greent
        self.omnicorp = greent.omnicorp

    def term_to_term(self,node_a,node_b):
        articles = self.omnicorp.get_shared_pmids(node_a, node_b)
        logger.debug(f'OmniCorp {node_a.identifier} {node_b.identifier} -> {len(articles)}')
        #Even if articles = [], we want to make an edge for the cache. We can decide later to write it or not.
        pmids = [f'PMID:{x.split("/")[-1]}' for x in articles]
        predicate=LabeledID('omnicorp:1', 'literature_co-occurrence')
        ke = KEdge(node_a, node_b, 'omnicorp.term_to_term', dt.now(), predicate,predicate,
                   f'{node_a.identifier},{node_b.identifier}',publications=pmids, is_support=True)
        return ke

    def generate_all_edges(self, nodelist):
        results = self.omnicorp.get_all_shared_pmids(nodelist)
        predicate=LabeledID('omnicorp:1', 'literature_co-occurrence')
        edges = [ KEdge(k[0], k[1], 'omnicorp.term_to_term', dt.now(), predicate,predicate,
                        f'{k[0].identifier},{k[1].identifier}', publications=v, is_support=True)
                  for k,v in results.items()]
        return edges


    def get_node_info(self,node):
        count = self.omnicorp.count_pmids(node)
        return {'omnicorp_article_count': count}

    def prepare(self,nodes):
        goodnodes = list(filter(lambda n: self.omnicorp.get_omni_identifier(n) is not None, nodes))
        return goodnodes
