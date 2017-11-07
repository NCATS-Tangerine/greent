import logging
import requests
import pprint
import urllib
from greent.service import Service
from greent.service import ServiceContext
from greent.util import Text
from reasoner.graph_components import KNode,KEdge
from reasoner import node_types

class QuickGo(Service):

    def __init__(self, context):
        super(QuickGo, self).__init__("quickgo", context)

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
                        results.append( ( self.get_edge (r, predicate=xrel['relation']), KNode (xrel['id'], node_types.CELL)) )
        return results

def test ():
    q = QuickGo (ServiceContext.create_context ())
    r = q.go_term_xontology_relationships (KNode("GO:0002551", node_types.PROCESS))
    pprint.pprint (r)
    r = q.go_term_xontology_relationships (KNode("GO.BIOLOGICAL_PROCESS:0042626", node_types.PROCESS))
    pprint.pprint (r)

if __name__ == '__main__':
    test ()
