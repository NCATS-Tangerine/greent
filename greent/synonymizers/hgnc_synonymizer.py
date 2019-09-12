from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text
from builder.question import LabeledID

def synonymize(node,gt):

    if Text.get_curie(node.id).upper() == 'UNIPROTKB':
        new_ids = gt.uniprot.get_synonyms(node.id)
        if len(new_ids) > 0:
            labeled_ids = [ LabeledID(identifier=h, label='') for h in new_ids ]
            node.synonyms.update(labeled_ids)
            node.id = new_ids[0]
    if Text.get_curie(node.id).upper() != 'UNIPROTKB':
        g_synonyms = gt.hgnc.get_synonyms(node.id)
    else:
        g_synonyms = set()
    return g_synonyms
