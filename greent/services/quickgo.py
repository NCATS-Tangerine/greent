import logging
import requests
import pprint
import urllib
from greent.service import Service
from greent.service import ServiceContext
from greent.util import Text
from greent.graph_components import KNode,KEdge
from greent import node_types

class QuickGo(Service):

    def __init__(self, context):
        super(QuickGo, self).__init__("quickgo", context)

    #TODO: Rename to reflect that this only returns cells?  See what else we can get?
    #Applies also to the annotation_extension functions
    def go_term_xontology_relationships(self, node):
        #Many of the nodes coming in will be something like GO.BIOLOGICAL_PROCESS:0042626 and
        # need to be downgraded to just GO
        url = "{0}/QuickGO/services/ontology/go/terms/GO:{1}/xontologyrelations".format (self.url, Text.un_curie(node.identifier))
        response = requests.get(url).json ()
        results = []
        for r in response['results']:
            if 'xRelations' in r:
                for xrel in r['xRelations']:
                    if xrel['id'].startswith('CL:'):
                        results.append( ( self.get_edge (r, predicate=xrel['relation']), KNode (xrel['id'], node_types.CELL, label = xrel['term']) ))
        return results

    def go_term_annotation_extensions(self,node):
        """This is playing a little fast and loose with the annotations.  Annotations relate a gene to a go term,
        and they can have an extension like occurs_in(celltype). Technically, that occurs_in only relates to that
        particular gene/go combination.  But it's the only way to traverse from neurotransmitter release to neurons 
        that is currently available"""
        url = '{0}/QuickGO/services/annotation/search?includeFields=goName&goId=GO:{1}&taxonId=9606&extension=occurs_in(CL)'.format( self.url, Text.un_curie(node.identifier)) 
        response = requests.get(url).json()
        results = []
        cell_ids = set()
        for r in response['results']:
            for e in r['extensions']:
                for c in e['connectedXrefs']:
                    if c['db'] == 'CL':
                        if c['id'] not in cell_ids:
                            results.append( (self.get_edge( r, predicate=c['qualifier']), 
                                KNode( 'CL:{}'.format(c['id']), node_types.CELL ) ) )
                            cell_ids.add(c['id'])
        return results


