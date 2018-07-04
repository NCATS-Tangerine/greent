from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text
from builder.question import LabeledThing

def synonymize(node,gt):
    if not node.type == node_types.GENE:
        raise Exception("Incorrect node type")
    if Text.get_curie(node.curie).upper() == 'UNIPROTKB':
        new_ids = gt.uniprot.get_synonyms(node.curie)
        if len(new_ids) > 0:
            labeled_ids = [ LabeledThing(identifier=h, label='') for h in new_ids ]
            node.synonyms.update(labeled_ids)
            node.curie = new_ids[0]
    if Text.get_curie(node.curie).upper() != 'UNIPROTKB':
        g_synonyms = gt.hgnc.get_synonyms(node.curie)
    else:
        g_synonyms = set()
    return g_synonyms
