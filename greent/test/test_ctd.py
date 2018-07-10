import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.graph_components import LabeledID
from greent.conftest import rosetta

@pytest.fixture()
def ctd(rosetta):
    ctd = rosetta.core.ctd
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
    input_node.add_synonyms(set([LabeledID("MESH:D000068579","blah")]))
    results = ctd.drug_to_gene(input_node)
    for edge,node in results:
        assert node.node_type == node_types.GENE
    result_ids = [ node.identifier for edge,node in results]
    assert 'NCBIGENE:5743' in result_ids #Cox2 for a cox2 inhibitor

def test_gene_to_drug_unique(ctd):
    input_node=KNode("NCBIGENE:345",node_types.GENE) #APOC3
    results = ctd.gene_to_drug(input_node)
    outputs = [ (e.standard_predicate,n.identifier) for e,n in results]
    total = len(outputs)
    unique = len(set(outputs))
    found = False
    for e,n in results:
        if n.identifier=='MESH:D004958':
            found = True
            assert n.label == 'Estradiol'
    assert total == unique

def test_gene_to_drug_ACHE(ctd):
    input_node=KNode("NCBIGENE:43",node_types.GENE) #ACHE
    results = ctd.gene_to_drug(input_node)
    outputs = [ (e.standard_predicate,n.identifier) for e,n in results]
    total = len(outputs)
    unique = len(set(outputs))
    found = False
    for e,n in results:
        if (n.identifier == 'MESH:D003976'):
            print(e)
    assert total == unique


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

def test_chemical_to_gene_glutathione(ctd):
    input_node = KNode("MESH:D006861", node_types.DRUG)
    results = ctd.drug_to_gene(input_node)
    for edge,node in results:
        assert node.node_type == node_types.GENE
    for edge, node in results:
        if node == edge.object_node:
            direction='+'
        elif node == edge.subject_node:
            direction = '-'
        else:
            print("wat")
        print(edge.original_predicate.identifier, edge.standard_predicate.identifier, node.identifier, direction)
    result_ids = [ node.identifier for edge,node in results]
    assert 'NCBIGENE:5743' in result_ids #Cox2 for a cox2 inhibitor

def test_disease_to_exposure(ctd):
    input_node = KNode("MESH:D001249", node_types.DISEASE, label='Asthma')
    results = ctd.disease_to_exposure(input_node)
    ddt = None
    for edge,node in results:
        assert node.node_type == node_types.DRUG
        assert edge.standard_predicate.identifier != 'GAMMA:0'
        if node.identifier == 'MESH:D003634':
            ddt = node
    assert len(results) > 0
    assert ddt is not None
    assert ddt.label == 'DDT'

def test_disease_to_chemical(ctd):
    input_node = KNode("MESH:D001249", node_types.DISEASE, label='Asthma')
    results = ctd.disease_to_chemical(input_node)
    ddt = None
    for edge,node in results:
        assert node.node_type == node_types.DRUG
        assert edge.standard_predicate.identifier != 'GAMMA:0'
