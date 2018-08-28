import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.graph_components import LabeledID
from greent.conftest import rosetta


@pytest.fixture()
def ctd(rosetta):
    ctd = rosetta.core.ctd
    return ctd


def test_gene_to_drug_and_back(ctd):
    input_node = KNode('MESH:D003976', type=node_types.GENE, name='Diazinon')
    results = ctd.drug_to_gene(input_node)
    results = list(filter(lambda en: en[1].id == 'NCBIGENE:5243', results))
    dgedges = set([e.original_predicate.label for e, n in results])
    input_node_2 = KNode('NCBIGENE:5243', type=node_types.GENE, name='ABCB1')
    results = ctd.gene_to_drug(input_node_2)
    results = list(filter(lambda en: en[1].id == 'MESH:D003976', results))
    gdedges = set([e.original_predicate.label for e, n in results])
    for dge in dgedges:
        print('Drug->Gene', dge)
    for gde in gdedges:
        print('Gene->Drug', gde)
    #assert False
    assert dgedges == gdedges


def test_drugname_to_mesh(ctd):
    nodes = ctd.drugname_string_to_drug("Celecoxib")
    assert len(nodes) == 1
    assert nodes[0].type == node_types.CHEMICAL_SUBSTANCE
    assert nodes[0].id == 'MESH:D000068579'


def test_drugname_to_mesh_wacky_caps(ctd):
    nodes = ctd.drugname_string_to_drug("cElEcOxIb")
    assert len(nodes) == 1
    assert nodes[0].type == node_types.CHEMICAL_SUBSTANCE
    assert nodes[0].id == 'MESH:D000068579'


def test_drugname_to_mesh_synonym(ctd):
    nodes = ctd.drugname_string_to_drug('2,5-dimethyl-celecoxib')
    assert len(nodes) == 1
    assert nodes[0].type == node_types.CHEMICAL_SUBSTANCE
    assert nodes[0].id == 'MESH:C506698'


def test_drugname_to_mesh_synonym_bar(ctd):
    """Make sure we can find a synonym in a long string of synonyms"""
    nodes = ctd.drugname_string_to_drug('DFLDEHPROSTA')
    assert len(nodes) == 1
    assert nodes[0].type == node_types.CHEMICAL_SUBSTANCE
    assert nodes[0].id == 'MESH:C024526'


def test_drug_to_gene_simple(ctd):
    input_node = KNode("MESH:D000068579", type=node_types.CHEMICAL_SUBSTANCE)
    results = ctd.drug_to_gene(input_node)
    for _, node in results:
        assert node.type == node_types.GENE
    result_ids = [node.id for edge, node in results]
    assert 'NCBIGENE:5743' in result_ids  # Cox2 for a cox2 inhibitor


def test_drug_to_gene_synonym(ctd):
    # Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("DB:FakeID", type=node_types.CHEMICAL_SUBSTANCE)
    input_node.add_synonyms(set([LabeledID(identifier="MESH:D000068579", label="blah")]))
    results = ctd.drug_to_gene(input_node)
    for _, node in results:
        assert node.type == node_types.GENE
    result_ids = [node.id for edge, node in results]
    assert 'NCBIGENE:5743' in result_ids  # Cox2 for a cox2 inhibitor


def test_gene_to_drug_unique(ctd):
    input_node = KNode("NCBIGENE:345", type=node_types.GENE)  # APOC3
    results = ctd.gene_to_drug(input_node)
    #We would rather have this, but right now it's loosing too much information
    #outputs = [(e.standard_predicate, n.id) for e, n in results]
    outputs = [(e.original_predicate, n.id) for e, n in results]
    total = len(outputs)
    unique = len(set(outputs))
    for _, n in results:
        if n.id == 'MESH:D004958':
            assert n.name == 'Estradiol'
    assert total == unique


def test_gene_to_drug_ACHE(ctd):
    input_node = KNode("NCBIGENE:43", type=node_types.GENE)  # ACHE
    results = ctd.gene_to_drug(input_node)
    #See note in test_gene_to_drug_unique
    outputs = [(e.original_predicate, n.id) for e, n in results]
    total = len(outputs)
    unique = len(set(outputs))
    for e, n in results:
        if (n.id == 'MESH:D003976'):
            print(e)
    assert total == unique


def test_gene_to_drug_synonym(ctd):
    # Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("DB:FakeID", type=node_types.GENE)
    input_node.add_synonyms(set(["NCBIGene:5743"]))
    results = ctd.gene_to_drug(input_node)
    for _, node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
    result_ids = [node.id for edge, node in results]
    assert 'MESH:D000068579' in result_ids  # Cox2 for a cox2 inhibitor


def test_artemether_to_gene(ctd):
    mesh = 'MESH:C032942'
    input_node = KNode(mesh, type=node_types.CHEMICAL_SUBSTANCE)
    results = ctd.drug_to_gene(input_node)
    for _, node in results:
        assert node.type == node_types.GENE
    result_ids = [node.id for edge, node in results]
    assert 'NCBIGENE:9970' in result_ids


def test_chemical_to_gene_glutathione(ctd):
    input_node = KNode("MESH:D006861", type=node_types.CHEMICAL_SUBSTANCE)
    results = ctd.drug_to_gene(input_node)
    for edge, node in results:
        assert node.type == node_types.GENE
    for edge, node in results:
        if node.id == edge.target_id:
            direction = '+'
        elif node.id == edge.source_id:
            direction = '-'
        else:
            print("wat")
        print(edge.original_predicate.identifier, edge.standard_predicate.identifier, node.id, direction)
    result_ids = [node.id for edge, node in results]
    assert 'NCBIGENE:5743' in result_ids  # Cox2 for a cox2 inhibitor


def test_disease_to_exposure(ctd):
    input_node = KNode("MESH:D001249", type=node_types.DISEASE, name='Asthma')
    results = ctd.disease_to_exposure(input_node)
    ddt = None
    for edge, node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
        if node.id == 'MESH:D003634':
            ddt = node
    assert len(results) > 0
    assert ddt is not None
    assert ddt.name == 'DDT'


def test_disease_to_chemical(ctd):
    input_node = KNode("MESH:D001249", type=node_types.DISEASE, name='Asthma')
    results = ctd.disease_to_chemical(input_node)
    for edge, node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
