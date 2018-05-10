from greent.graph_components import KNode
from greent.conftest import rosetta
from greent import node_types
from greent.util import Text
from builder import lookup_utils

def test_mesh_synonymization(rosetta):
    gt = rosetta.core
    r = lookup_utils.lookup_drug_by_name("FLECAINIDE",gt)
    print(r)
    assert 0==1
