import logging
import requests
from greent.service import Service
from greent.util import Text,LoggingUtil
from greent.graph_components import KNode,LabeledID
from greent import node_types
from datetime import datetime as dt

logger = LoggingUtil.init_logging (__file__)

class QuickGo(Service):

    def __init__(self, context):
        super(QuickGo, self).__init__("quickgo", context)

    def get_predicate(self, p_label):
        labels2identifiers={'occurs_in': 'BFO:0000066',
                'results_in_movement_of': 'RO:0002565',
                'results in developmental progression of':'RO:0002295',
                'results in development of':'RO:0002296',
                'results in formation of':'RO:0002297',
                'results in synthesis of':'RO:0002587',
                'results in assembly of':'RO:0002588',
                'results in morphogenesis of':'RO:0002298',
                'results in maturation of':'RO:0002299',
                'results in acquisition of features of':'RO:0002315',
                'results in growth of':'RO:0002343',
                'results in commitment to':'RO:0002348',
                'results in determination of':'RO:0002349',
                'results in structural organization of':'RO:0002355',
                'results in specification of':'RO:0002356',
                'results in developmental induction of':'RO:0002357',
                'results in ending of':'RO:0002552',
                'results in disappearance of':'RO:0002300',
                'results in developmental regression of':'RO:0002301',
                'results in closure of':'RO:0002585',
                }
        try:
            return LabeledID(labels2identifiers[p_label],p_label)
        except:
            logger.warn(p_label)
            return LabeledID(f'GO:{p_label}',p_label)

    def standardize_predicate(self, predicate):
        """Fall back to a catch-all if we can't find a specific mapping"""
        try:
            super(QuickGo, self).standardize_predicate(predicate)
        except:
            return super(QuickGo,self).standardize_predicate(self.get_predicate('occurs_in'))

    #TODO: Rename to reflect that this only returns cells?  See what else we can get?
    #Applies also to the annotation_extension functions
    def go_term_xontology_relationships(self, go_node):
        #Many of the nodes coming in will be something like GO.BIOLOGICAL_PROCESS:0042626 and
        # need to be downgraded to just GO
        url = "{0}/QuickGO/services/ontology/go/terms/GO:{1}/xontologyrelations".format (self.url, Text.un_curie(go_node.identifier))
        response = requests.get(url).json ()
        results = []
        if not 'results' in response:
            return results
        for r in response['results']:
            if 'xRelations' in r:
                for xrel in r['xRelations']:
                    if xrel['id'].startswith('CL:'):
                        predicate = self.get_predicate(xrel['relation'])
                        cell_node = KNode (xrel['id'], node_types.CELL, label = xrel['term']) 
                        edge = self.create_edge(go_node, cell_node,'quickgo.go_term_xontology_relationships',go_node.identifier,predicate,url = url)
                        results.append( ( edge , cell_node))
        return results

    def go_term_annotation_extensions(self,go_node):
        """This is playing a little fast and loose with the annotations.  Annotations relate a gene to a go term,
        and they can have an extension like occurs_in(celltype). Technically, that occurs_in only relates to that
        particular gene/go combination.  But it's the only way to traverse from neurotransmitter release to neurons 
        that is currently available"""
        url = '{0}/QuickGO/services/annotation/search?includeFields=goName&goId=GO:{1}&taxonId=9606&extension=occurs_in(CL)'.format( self.url, Text.un_curie(go_node.identifier)) 
        response = requests.get(url).json()
        results = []
        cell_ids = set()
        if not 'results' in response:
            return results
        for r in response['results']:
            for e in r['extensions']:
                for c in e['connectedXrefs']:
                    if c['db'] == 'CL':
                        if c['id'] not in cell_ids:
                            predicate = self.get_predicate(c['qualifier'])
                            cell_node = KNode( 'CL:{}'.format(c['id']), node_types.CELL ) 
                            edge = self.create_edge(go_node, cell_node, 'quickgo.go_term_annotation_extensions',go_node.identifier,predicate,url = url)
                            results.append( (edge,cell_node ) )
                            cell_ids.add(c['id'])
        return results


