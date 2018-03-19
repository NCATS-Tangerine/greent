import pytest
from greent.graph_components import KNode
from greent.services.chembio import ChemBioKS
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def chembio():
    chembio = ChemBioKS(ServiceContext.create_context())
    return chembio

def test_name_lookup(chembio):
    from greent.service import ServiceContext
    chembio = ChemBioKS(ServiceContext.create_context())
    input_node = KNode("DRUG_NAME:imatinib", node_types.DRUG_NAME)
    results = chembio.graph_drugname_to_pubchem( input_node )
    edge,node = results[0]
    assert node.identifier=='PUBCHEM:5291'
    assert node.node_type == node_types.DRUG
