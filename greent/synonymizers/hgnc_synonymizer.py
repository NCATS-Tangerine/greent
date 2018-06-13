from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text

def synonymize(node,gt):
    if not node.node_type == node_types.GENE:
        raise Exception("Incorrect node type")
    if Text.get_curie(node.identifier).upper() == 'UNIPROTKB':
        hgnc_ids = gt.uniprot.uniprot_2_hgnc(node.identifier)
        if len(hgnc_ids) > 0:
            hgnc_labeled_ids = [ LabeledID(h,'') for h in hgnc_ids ]
            node.add_synonyms(hgnc_labeled_ids)
            node.identifier = hgnc_ids[0]
    return gt.hgnc.get_synonyms(node.identifier)
    #node.add_synonyms(synonyms)
