from greent.graph_components import KNode
from greent.synonymizers.disease_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types
from greent.util import Text


def test_mondo_synonymization(rosetta):
    #Niemann Pick Disease (not type C)
    node = KNode('MONDO:0001982', type=node_types.DISEASE)
    synonyms = synonymize(node,rosetta.core)
    assert len(synonyms) > 10
    node.add_synonyms(synonyms)
    doids = node.get_synonyms_by_prefix('DOID')
    assert len(doids) == 1
    assert doids.pop() == 'DOID:14504'
    meshes = node.get_synonyms_by_prefix('MESH')
    assert len(meshes) == 2
    assert 'MeSH:D009542' in meshes
    assert 'MeSH:D052556' in meshes
    assert Text.get_curie(node.id) == 'MONDO'
    #This is not really what the synonymizer does.  The synonymizer returns synonyms, not modifies the node.
    #assert node.name == 'Niemann-Pick Disease'

def test_mondo_synonymization_2(rosetta):
    node = KNode('MONDO:0005737', type=node_types.DISEASE)
    synonyms = synonymize(node,rosetta.core)
    assert len(synonyms) > 1
    node.add_synonyms(synonyms)
    doids = node.get_synonyms_by_prefix('DOID')
    assert len(doids) == 1
    meshes = node.get_synonyms_by_prefix('MESH')
    assert len(meshes) > 0
    assert Text.get_curie(node.id) == 'MONDO'

#This test doesn't currently pass because OXO hasn't integrated MONDO yet
def future_test_disease_normalization(rosetta):
    node = KNode('DOID:4325', type=node_types.DISEASE)
    synonyms = synonymize(node,rosetta.core)
    print( synonyms )
    node.add_synonyms(synonyms)
    mondos = node.get_synonyms_by_prefix('MONDO')
    assert len(mondos) > 0
    assert Text.get_curie(node.id) == 'MONDO'
