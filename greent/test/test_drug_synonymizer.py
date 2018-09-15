from greent.graph_components import KNode, LabeledID
from greent.synonymizers.substance_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types

def test_no_result(rosetta):
    #Make sure we don't throw an exception
    node = KNode('CHEBI:38253',type=node_types.CHEMICAL_SUBSTANCE)
    synonyms = synonymize(node,rosetta.core)
    assert True

def test_mesh_synonymization(rosetta):
    node = KNode('MESH:C032942', type=node_types.CHEMICAL_SUBSTANCE)
    synonyms = synonymize(node,rosetta.core)
    for s in synonyms:
        assert isinstance(s, LabeledID)

def test_chembl_synonymization(rosetta):
    node = KNode('CHEMBL:CHEMBL744', type=node_types.CHEMICAL_SUBSTANCE)
    synonyms = synonymize(node,rosetta.core)
    for s in synonyms:
        assert isinstance(s, LabeledID)

def test_from_each(rosetta):
    #For these tests, you should know that ADAPALENE has these identifiers
    start_identifiers=['MESH:D000068816','CHEMBL:CHEMBL1265','PUBCHEM:60164']
    #And we should be able to start with any and get the others
    for start in start_identifiers:
        node = KNode(start, type=node_types.CHEMICAL_SUBSTANCE, name='Adapalene')
        synonyms = synonymize(node,rosetta.core)
        sids = [ s.identifier for s in synonyms ]
        #for start_id in start_identifiers:
        #    assert start_id in sids
        print(start, sids)
