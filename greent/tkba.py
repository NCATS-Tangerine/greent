import requests
import json
from greent.service import Service
from greent.service import ServiceContext
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KNode,KEdge
from greent import node_types

logger = LoggingUtil.init_logging (__name__)

class TranslatorKnowledgeBeaconAggregator(Service):
    """ Use Translator Knowledge Beacons via the aggregator. """

    def __init__(self, context):
        super(TranslatorKnowledgeBeaconAggregator, self).__init__("tkba", context)

    def request_concept (self, concept):
        #Without quotes around the keyword, this function treats space as a delimiter...
        keyword = Text.un_curie (concept.identifier)
        keyword = '"{0}"'.format (keyword) if ' ' in keyword else keyword
        url = '{0}/concepts?keywords={1}'.format (self.url, keyword)
        return requests.get (url).json ()

    def name_to_doid (self, name):
        result = []
        response = self.request_concept (name)
        seen = {}
        for r in response:
            for a in r['aliases']:
                if a.startswith ("DOID:"):
                    if not a in seen:
                        logger.debug ("      -- appending a {}".format (a))
                        result.append ( ( self.get_edge (r, predicate='name_to_doid'), KNode(a, node_types.DISEASE ) ) )
                        seen[a] = a
        return result

    def name_to_drugbank (self, name):
        response = self.request_concept (name)
        result = []
        seen = {}
        for r in response:
            for a in r['aliases']:
                if a.startswith ("DRUGBANK:"):
                    if not a in seen:
                        result.append ( ( self.get_edge (r, predicate='name_to_drugbank'), KNode(a, node_types.DRUG) ) )
                        seen[a] = a
        return list(set(result))
        
    def name_to_mesh (self, name):
        result = []
        response = self.request_concept (name)
        seen = {}
        for r in response:
            for a in r['aliases']:
                if a.startswith ("MESH:"):
                    if not a in seen:
                        #TODO: not sure what node type should be here...
                        result.append ( ( self.get_edge (r, predicate='name_to_mesh'), KNode(a, name.node_type) ) )
                        seen[a] = a
        return list(set(result))
    
    def name_to_mesh_disease (self, name):
        response = self.request_concept (name)
        is_a_disease = False
        for r in response:
            for a in r['aliases']:
                if a.startswith ("DOID:"):
                    is_a_disease = True
                    break
        result = []
        if is_a_disease:
            seen = {}
            for r in response:
                for a in r['aliases']:
                    if a.startswith ("MESH:"):
                        if not a in seen:
                            a = a.replace ("MESH:", "MESH.DISEASE:")
                            result.append ( ( self.get_edge (r, predicate='name_to_mesh_disease'), \
                                KNode(a, node_types.DISEASE) ) )
                            seen[a] = a
        return list(set(result))

if __name__ == "__main__":
    t = TranslatorKnowledgeBeaconAggregator (ServiceContext.create_context ())
    print (t.name_to_mesh_disease (KNode("NAME.DISEASE:asthma", node_types.NAME_DISEASE)))
    print (t.name_to_doid (KNode("NAME.DISEASE:asthma", node_types.DISEASE)))
