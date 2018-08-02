from greent.conftest import rosetta
from greent import node_types
from greent.util import Text
from builder import lookup_utils

def test_mesh_synonymization(rosetta):
    gt = rosetta.core
    r = lookup_utils.lookup_drug_by_name("FLECAINIDE",gt)
    assert len(r) == 3
    assert 'MESH:D005424' in r
    assert 'CHEMBL:CHEMBL652' in r
    assert 'PUBCHEM:3356' in r
