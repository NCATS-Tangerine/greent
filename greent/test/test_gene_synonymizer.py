from greent.graph_components import KNode
from greent.synonymizers.hgnc_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types

def test_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:O75381', node_types.GENE, label='PEX14')
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:8856'
    assert node.identifier == 'HGNC:8856'

def test_hgnc(rosetta):
    """Observed an error for this id, is it transient?"""
    node = KNode('HGNC:8599', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert node.label == 'PANX1'

