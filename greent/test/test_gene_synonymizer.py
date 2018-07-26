from greent.graph_components import KNode
from greent.synonymizers.hgnc_synonymizer import synonymize
from greent.conftest import rosetta
from greent import node_types
from greent.synonymizers import hgnc_synonymizer

def test_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:O75381', type=node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:8856'
    assert node.id == 'HGNC:8856'
    assert node.name == 'PEX14'

def test_crappy_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:A0A024QZH5', type=node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:18859'
    assert node.id == 'HGNC:18859'
    assert node.name == 'SPHK2'

def test_failing_uniprot(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:P01160', type=node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:7939'
    assert node.id == 'HGNC:7939'
    assert node.name == 'NPPA'

def test_failing_uniprot_2(rosetta):
    """Do we correctly synonymize if all we have is a UniProtKB identifier?"""
    node = KNode('UniProtKB:P14416', type=node_types.GENE, name='')
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert len(hgnc) == 1
    assert hgnc.pop() == 'HGNC:3023'
    assert node.id == 'HGNC:3023'
    assert node.name == 'DRD2'

def test_hgnc(rosetta):
    """Observed an error for this id, is it transient?"""
    node = KNode('HGNC:8599', type=node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert node.name == 'PANX1'

def test_hgnc_label(rosetta):
    """Do I get a label back?"""
    node = KNode('HGNC:18729', type=node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    hgnc = node.get_synonyms_by_prefix('HGNC')
    assert node.name is not None
    assert node.name != ''

def test_gene_synonymizer(rosetta):
    node = KNode('NCBIGENE:57016', type=node_types.GENE)
    results = hgnc_synonymizer.synonymize(node,rosetta.core)
    print(results)
    assert len(results) > 0
