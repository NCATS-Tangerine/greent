import pytest
from greent.graph_components import KNode
from greent.services.quickgo import QuickGo
from greent.servicecontext import ServiceContext
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def quickgo(rosetta):
    quickgo = rosetta.core.quickgo
    return quickgo

def test_xontology_relationships(quickgo):
    #Mast Cell Chemotaxis
    r = quickgo.go_term_to_cell_xontology_relationships (KNode("GO:0002551", type=node_types.BIOLOGICAL_PROCESS))
    assert len(r) == 1
    assert r[0][1].type == node_types.CELL
    #Mast Cells
    assert r[0][1].id == 'CL:0000097'

def test_extensions(quickgo):
    #positive regulation of action potential
    r = quickgo.go_term_to_cell_annotation_extensions (KNode("GO:0045760", type=node_types.BIOLOGICAL_PROCESS))
    types = set([n.type for e,n in r])
    assert len(types) > 0
    assert node_types.CELL in types
    for cell_id in ['CL:0000746', 'CL:2000097']:
        myedges = list(filter( lambda en: en[1].id==cell_id, r))
        assert len(myedges) == 1
        assert myedges[0][0].standard_predicate is not None

def xtest_extensions_long(quickgo):
    #positive regulation of action potential
    r = quickgo.go_term_to_cell_annotation_extensions (KNode("GO:0044267", type=node_types.BIOLOGICAL_PROCESS))
    #types = set([n.type for e,n in r])
    assert len(r) > 0
    #assert node_types.CELL in types
    #for cell_id in ['CL:0000746', 'CL:2000097']:
    #    myedges = list(filter( lambda en: en[1].id==cell_id, r))
    #    assert len(myedges) == 1
    #    assert myedges[0][0].standard_predicate is not None

def test_reverse_extensions(quickgo):
    r = quickgo.cell_to_go_term_annotation_extensions(KNode("CL:0002131", type=node_types.CELL))
    types = set([n.type for e,n in r])
    assert len(types) == 1
    assert node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY in types
    myedges = list(filter( lambda en: en[1].id=='GO:0017080' , r))
    assert len(myedges) == 1
    assert myedges[0][0].standard_predicate is not None
    assert myedges[0][1].name=='sodium channel regulator activity'


def test_reverse_extensions(quickgo):
    r = quickgo.cell_to_go_term_annotation_extensions(KNode("CL:0002189", type=node_types.CELL))
    assert len(r) > 0
    types = set([n.type for e,n in r])
    assert len(types) == 1
    assert node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY in types
    print( r )
    #myedges = list(filter( lambda en: en[1].id=='GO:0017080' , r))
    #assert len(myedges) == 1
    #assert myedges[0][0].standard_predicate is not None
    #assert myedges[0][1].name=='sodium channel regulator activity'


def test_extensions_bp(quickgo):
    #Neurotransmitter secretion
    r = quickgo.go_term_to_cell_annotation_extensions (KNode("GO.BIOLOGICAL_PROCESS:0007269", type=node_types.BIOLOGICAL_PROCESS))
    types = set([n.type for e,n in r])
    assert len(types) == 1
    assert node_types.CELL in types
    identifiers = set([n.id for e,n in r])
    assert len(identifiers) == 3
    assert 'CL:0000700' in identifiers #Dopaminergic neuron
    assert 'CL:0002608' in identifiers #Hippocampal neuron
    assert 'CL:1001571' in identifiers #Hippocampal pyramidal neuron


def test_go_to_gene(quickgo):
    r = quickgo.go_term_to_gene_annotation (KNode("GO:0007165", type=node_types.BIOLOGICAL_PROCESS))
    for e,k in r:
        assert k.type == node_types.GENE
    assert len(r) > 25


def test_go_to_cellular_component(quickgo):
    r = quickgo.gene_to_cellular_component(KNode('UniProtKB:P30518', type = node_types.GENE))
    for e,k in r:
        assert k.type == node_types.CELLULAR_COMPONENT
    assert len(r) >  2


