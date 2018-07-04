from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text
from builder.question import LabeledThing

def synonymize(node,gt):
    """The main thing to worry about for cells is that we get a label."""
    if not node.node_type == node_types.CELL:
        raise Exception("Incorrect node type")
    currentsynonyms = node.get_labeled_ids_by_prefix('CL')
    new_syns = set()
    for csim in currentsynonyms:
        if csim.label is None or csim.label == '':
            label = gt.uberongraph.cell_get_cellname(csim.identifier)[0]['cellLabel']
            new_syns.add(LabeledThing(identifier=csim.identifier, label=label))
    return new_syns
