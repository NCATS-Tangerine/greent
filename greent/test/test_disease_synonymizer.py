from greent.graph_components import KNode
from greent.synonymizers.disease_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types


def test_mondo_synonymization(rosetta):
    node = KNode('MONDO:0009757',node_types.DISEASE)
    synonymize(node,rosetta.core)
    assert len(node.synonyms) > 10
    doids = node.get_synonyms_by_prefix('DOID')
    assert len(doids) == 1
    assert doids.pop() == 'DOID:14504'
    meshes = node.get_synonyms_by_prefix('MESH')
    assert len(meshes) == 3
    assert 'MeSH:C564941' in meshes
    assert 'MeSH:D009542' in meshes
    assert 'MeSH:D052556' in meshes
