import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta


@pytest.fixture()
def kegg(rosetta):
    kegg = rosetta.core.kegg
    return kegg

def test_chem_to_reaction(kegg):
    hete = KNode('KEGG.COMPOUND:C04805', name="5-HETE", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_reaction(hete)
    assert len(results)  == 1
    assert results[0] == 'rn:R07034'

def test_rxn_to_chem(kegg):
    results = kegg.reaction_get_chemicals('rn:R07034')
    assert len(results) == 5

def test_get_reaction(kegg):
    reaction = kegg.get_reaction('rn:R07034')
    assert reaction['enzyme'] == 'EC:1.11.1.9'
    assert len(reaction['reactants']) == 2
    assert 'C00051' in reaction['reactants']
    assert 'C05356' in reaction['reactants']
    assert len(reaction['products']) == 3
    assert 'C00001' in reaction['products']
    assert 'C00127' in reaction['products']
    assert 'C04805' in reaction['products']

def test_chem_to_enzyme(kegg):
    hete = KNode('KEGG.COMPOUND:C04805', name="5-HETE", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(hete)
    assert len(results) == 1
    edge=results[0][0]
    node=results[0][1]
    assert node.id == 'EC:1.11.1.9'

def test_enzyme_to_chem(kegg):
    enzyme = KNode('EC:1.11.1.9', name="who", type=node_types.GENE)
    results = kegg.enzyme_get_chemicals(enzyme)
    assert len(results) == 8

