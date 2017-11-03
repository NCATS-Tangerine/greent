import logging
import requests
import pprint
import urllib
from greent.service import Service
from greent.service import ServiceContext
from util import Text
from reasoner.graph_components import KNode,KEdge

class QuickGo(Service):

    def __init__(self, context):
        super(QuickGo, self).__init__("quickgo", context)

    def go_term_xontology_relationships(self, node):
        url = "{0}/QuickGO/services/ontology/go/terms/{1}/xontologyrelations".format (self.url, node.identifier)
        response = requests.get(url).json ()
        return [ (
            self.get_edge (r, predicate=xrel['relation']),
            KNode (xrel['id'], 'F')
        ) for r in response['results'] for xrel in r['xRelations'] if xrel['id'].startswith ('CL:') ]
    
def test ():
    q = QuickGo (ServiceContext.create_context ())
    r = q.go_term_xontology_relationships (KNode("GO:0002551", 'G'))
    pprint.pprint (r)

if __name__ == '__main__':
    test ()
