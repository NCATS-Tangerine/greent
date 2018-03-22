import pytest
from greent.graph_components import KNode
from greent.ontologies.go import GO
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def go():
    go = GO(ServiceContext.create_context())
    return go

def test_biological_process(go):
    #Mast Cell Chemotaxis
    go_id = 'GO:0002551'
    assert go.is_biological_process(go_id)
    assert not go.is_cellular_component(go_id)
    assert not go.is_molecular_function(go_id)

def test_cellular_component(go):
    #Myelin Sheath
    go_id = 'GO:0043209'
    assert not go.is_biological_process(go_id)
    assert go.is_cellular_component(go_id)
    assert not go.is_molecular_function(go_id)

def test_molecular_function(go):
    #FBXO Family Binding Protein
    go_id = 'GO:0098770'
    assert not go.is_biological_process(go_id)
    assert not go.is_cellular_component(go_id)
    assert go.is_molecular_function(go_id)

