import pytest
from greent.graph_components import KNode
from greent.services.quickgo import QuickGo
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def quickgo():
    quickgo = QuickGo(ServiceContext.create_context())
    return quickgo

def test_xontology_relationships(quickgo):
    #Mast Cell Chemotaxis
    r = quickgo.go_term_xontology_relationships (KNode("GO:0002551", node_types.PROCESS))
    assert len(r) == 1
    assert r[0][1].node_type == node_types.CELL
    #Mast Cells
    assert r[0][1].identifier == 'CL:0000097'

#Do we still want to allow funky go types?
def test_allow_funky_gotypes(quickgo):
    r0 = quickgo.go_term_xontology_relationships (KNode("GO:0002551", node_types.PROCESS))
    r1 = quickgo.go_term_xontology_relationships (KNode("GO.BIOLOGICAL_PROCESS:0002551", node_types.PROCESS))
    assert len(r0) == len(r1) == 1
    assert r0[0][0] == r1[0][0]
    assert r0[0][1] == r1[0][1]

def test_extensions(quickgo):
    #Neurotransmitter secretion
    r = quickgo.go_term_annotation_extensions (KNode("GO.BIOLOGICAL_PROCESS:0007269", node_types.PROCESS))
    types = set([n.node_type for e,n in r])
    assert len(types) == 1
    assert node_types.CELL in types
    identifiers = set([n.identifier for e,n in r])
    assert len(identifiers) == 3
    assert 'CL:0000700' in identifiers #Dopaminergic neuron
    assert 'CL:0002608' in identifiers #Hippocampal neuron
    assert 'CL:1001571' in identifiers #Hippocampal pyramidal neuron


