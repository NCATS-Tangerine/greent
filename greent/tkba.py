import requests
from greent.util import LoggingUtil
from reasoner.graph_components import KNode,KEdge

logger = LoggingUtil.init_logging (__name__)

class TranslatorKnowledgeBeaconAggregator:
    def __init__(self, url=""):
        self.url = url
    def name_to_doid (self, name):
        result = []
        url = "{0}/concepts?keywords={1}".format (self.url, name.identifier.split(':')[1])
#        print (url)
        response = requests.get (url).json ()
        for r in response:
            the_id = r['id']
            if the_id.startswith ("DOID:"):
                result.append ( ( KEdge ('tkba', 'queried', r), KNode(the_id, 'D') ) )
        return result
    def get_concepts (self, concept):
        result = []
        url = "{0}/concepts?keywords={1}".format (self.url, concept.identifier)
        response = requests.get (url).json ()
        for r in response:
            semantic_group = r['semanticGroup']
            the_id = r['id']
            if semantic_group == 'DISO':
                result.append ( ( KEdge ('tkba', 'queried', r), KNode(the_id, 'D') ) )
            elif semantic_group == 'PHYS':
                if the_id.startswith ("KEGG"):
                    result.append ( (KEdge ('tkba', 'queried', r), KNode(the_id, 'P') ) )
        return result
