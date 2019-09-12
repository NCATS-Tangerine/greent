from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text

def synonymize(node,gt):
      
    synonyms = set()
    caids = node.get_synonyms_by_prefix('CAID')
    if (caids):
        synonyms.update(gt.clingen.get_synonyms_by_caid(Text.un_curie(caids.pop())))
    else:
        synonyms.update(gt.clingen.get_synonyms_by_other_ids(node))

    return synonyms
