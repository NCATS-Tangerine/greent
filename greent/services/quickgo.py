import logging
import requests
from greent.service import Service
from greent.util import Text
from greent.graph_components import KNode,LabeledID
from greent import node_types
from datetime import datetime as dt

class QuickGo(Service):

    def __init__(self, context):
        super(QuickGo, self).__init__("quickgo", context)

    def get_predicate(self, p_label):
        if p_label == 'occurs_in':
            return LabeledID('BFO:0000066',p_label)
        if p_label == 'results_in_movement_of':
            return LabeledID('RO:0002565',p_label)
        print(p_label)
        return LabeledID(f'GO:{p_label}',p_label)

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


