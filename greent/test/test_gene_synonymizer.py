from greent.graph_components import KNode
from greent.synonymizers.hgnc_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types
from greent.synonymizers import hgnc_synonymizer

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

def test_failing_uniprot_2(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:P14416', node_types.GENE, label='')
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:3023'
    assert node.identifier == 'HGNC:3023'
    assert node.label == 'DRD2'

def test_hgnc(rosetta):
    """Observed an error for this id, is it transient?"""
    node = KNode('HGNC:8599', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert node.label == 'PANX1'

def test_hgnc_label(rosetta):
    """Do I get a label back?"""
    node = KNode('HGNC:18729', node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert node.label is not None
    assert node.label != ''

def test_gene_synonymizer(rosetta):
    node = KNode('HGNC:18729', node_types.GENE)
    results = hgnc_synonymizer.synonymize(node,rosetta.core)
    print(results)
    assert len(results) > 0
