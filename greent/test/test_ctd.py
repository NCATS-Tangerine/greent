import pytest
from greent.graph_components import KNode
from greent.services.ctd import CTD
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def ctd():
    ctd = CTD(ServiceContext.create_context())
    return ctd

def test_drugname_to_mesh(ctd):
    nodes = ctd.drugname_string_to_drug("Celecoxib")
    assert len(nodes) == 1
    assert nodes[0].node_type == node_types.DRUG
    assert nodes[0].identifier == 'MESH:D000068579'

def test_drugname_to_mesh_wacky_caps(ctd):
    nodes = ctd.drugname_string_to_drug("cElEcOxIb")
    assert len(nodes) == 1
    assert nodes[0].node_type == node_types.DRUG
    assert nodes[0].identifier == 'MESH:D000068579'

def test_drugname_to_mesh_synonym(ctd):
    nodes=ctd.drugname_string_to_drug('2,5-dimethyl-celecoxib')
    assert len(nodes) == 1
    assert nodes[0].node_type == node_types.DRUG
    assert nodes[0].identifier == 'MESH:C506698'

def test_drugname_to_mesh_synonym_bar(ctd):
    """Make sure we can find a synonym in a long string of synonyms"""
    nodes=ctd.drugname_string_to_drug('DFLDEHPROSTA')
    assert len(nodes) == 1
    assert nodes[0].node_type == node_types.DRUG
    assert nodes[0].identifier == 'MESH:C024526'

def test_drug_to_gene_simple(ctd):
    input_node = KNode("MESH:D000068579", node_types.DRUG)
    results = ctd.drug_to_gene(input_node)
    for edge,node in results:
        assert node.node_type == node_types.GENE
    result_ids = [ node.identifier for edge,node in results]
    assert 'NCBIGENE:5743' in result_ids #Cox2 for a cox2 inhibitor

def test_drug_to_gene_synonym(ctd):
    #Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("DB:FakeID", node_types.DRUG)
    input_node.add_synonyms(set(["MESH:D000068579"]))
    results = ctd.drug_to_gene(input_node)
    for edge,node in results:
        assert node.node_type == node_types.GENE
    result_ids = [ node.identifier for edge,node in results]
    assert 'NCBIGENE:5743' in result_ids #Cox2 for a cox2 inhibitor

def test_gene_to_drug_synonym(ctd):
     #Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("DB:FakeID", node_types.GENE)
    input_node.add_synonyms(set(["NCBIGene:5743"]))
    results = ctd.gene_to_drug(input_node)
    for edge,node in results:
        assert node.node_type == node_types.DRUG
    result_ids = [ node.identifier for edge,node in results]
    assert 'MESH:D000068579' in result_ids #Cox2 for a cox2 inhibitor

def test_artemether_to_gene(ctd):
    mesh = 'MESH:C032942'
    input_node = KNode(mesh, node_types.DRUG)
    results = ctd.drug_to_gene(input_node)
    for edge,node in results:
        assert node.node_type == node_types.GENE
    result_ids = [ node.identifier for edge,node in results]
    assert 'NCBIGENE:9970' in result_ids #

