import pytest
from greent.graph_components import KNode
from greent.services.chembio import ChemBioKS
from greent.servicecontext import ServiceContext
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def chembio(rosetta):
    return rosetta.core.chembio

"""No longer relevant"""
'''
def test_name_lookup(chembio):
    input_node = KNode("DRUG_NAME:imatinib", node_types.DRUG_NAME)
    results = chembio.graph_drugname_to_pubchem( input_node )
    edge,node = results[0]
    assert node.identifier=='PUBCHEM:5291'
    assert node.node_type == node_types.DRUG
'''
