from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text

def synonymize(node,gt):
    if not node.node_type == node_types.GENE:
        raise Exception("Incorrect node type")
    if Text.get_curie(node.identifier).upper() == 'UNIPROTKB':
        new_ids = gt.uniprot.get_synonyms(node.identifier)
        if len(new_ids) > 0:
            labeled_ids = [ LabeledID(h,'') for h in new_ids ]
            node.add_synonyms(labeled_ids)
            node.identifier = new_ids[0]
    if Text.get_curie(node.identifier).upper() != 'UNIPROTKB':
        g_synonyms = gt.hgnc.get_synonyms(node.identifier)
    else:
        g_synonyms = set()
    return g_synonyms
