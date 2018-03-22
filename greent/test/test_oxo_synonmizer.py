from greent.graph_components import KNode
from greent.synonymizers.oxo_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types

def test_neuron(rosetta):
    node = KNode("CL:0000540", node_types.CELL)
    synonymize(node,rosetta.core)
    assert len(node.synonyms) >  10
    meshcell = node.get_synonyms_by_prefix("MESH")
    assert len(meshcell) == 1
    mid = list(meshcell)[0]
    assert mid == 'MeSH:D009474'
