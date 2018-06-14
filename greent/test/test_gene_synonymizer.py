from greent.graph_components import KNode
from greent.synonymizers.hgnc_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types

def test_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:O75381', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:8856'
    assert node.identifier == 'HGNC:8856'
    assert node.label == 'PEX14'

def test_crappy_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:A0A024QZH5', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:18859'
    assert node.identifier == 'HGNC:18859'
    assert node.label == 'SPHK2'

def test_failing_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:P01160', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:7939'
    assert node.identifier == 'HGNC:7939'
    assert node.label == 'NPPA'



def test_hgnc(rosetta):
    """Observed an error for this id, is it transient?"""
    node = KNode('HGNC:8599', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert node.label == 'PANX1'

