import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.graph_components import LabeledID
from greent.conftest import rosetta


@pytest.fixture()
def ctd(rosetta):
    ctd = rosetta.core.ctd
    return ctd

def test_expanded_drug_to_gene(ctd):
    input_node = KNode("MESH:D003976", type=node_types.CHEMICAL_SUBSTANCE, name="Diazinon")
    results = ctd.drug_to_gene_expanded(input_node)
    for edge,node in results:
        assert node.type == node_types.GENE
        print( edge.original_predicate, edge.standard_predicate )
        assert edge.standard_predicate.identifier != 'GAMMA:0'

def test_expanded_drug_to_gene_too_many(ctd):
    input_node = KNode("MESH:D001335", type=node_types.CHEMICAL_SUBSTANCE, name="question")
    results = ctd.drug_to_gene_expanded(input_node)
    print(len(results))
    from collections import defaultdict
    counter=defaultdict(int)
    for edge,node in results:
        assert node.type == node_types.GENE
        counter[edge.standard_predicate.identifier] += 1
        assert edge.standard_predicate.identifier != 'GAMMA:0'
    for e in counter:
        print(e,counter[e])

def test_expanded_drug_to_gene_glucose(ctd,rosetta):
    input_node = KNode("CHEBI:17234", type=node_types.CHEMICAL_SUBSTANCE, name="Glucose")
    rosetta.synonymizer.synonymize(input_node)
    results = ctd.drug_to_gene_expanded(input_node)
    assert len(results) > 0
    for edge,node in results:
        assert node.type == node_types.GENE
        print( edge.original_predicate, edge.standard_predicate, node.name )
        assert edge.standard_predicate.identifier != 'GAMMA:0'

def test_expanded_gene_to_drug_what(ctd,rosetta):
    input_node = KNode("HGNC:1305", type=node_types.GENE, name="C6ORF21")
    rosetta.synonymizer.synonymize(input_node)
    results = ctd.gene_to_drug_expanded(input_node)
    assert len(results) > 0
    for edge,node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
        print(edge, edge.standard_predicate)

def test_expanded_gene_to_drug(ctd,rosetta):
    input_node = KNode("HGNC:4558", type=node_types.GENE, name="GPX6")
    rosetta.synonymizer.synonymize(input_node)
    results = ctd.gene_to_drug_expanded(input_node)
    assert len(results) > 0
    for edge,node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
        print(edge, edge.standard_predicate)

def test_disease_to_chemical_fails(rosetta,ctd):
    input_node = KNode("MONDO:0009184", type=node_types.DISEASE, name='something')
    rosetta.synonymizer.synonymize(input_node)
    print(input_node.synonyms)
    results = ctd.disease_to_chemical(input_node)
    #Now, we're not returning the inferred ones.
    assert len(results) > 100
    for edge, node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'

def test_disease_to_chemical(rosetta,ctd):
    input_node = KNode("MONDO:0004979", type=node_types.DISEASE, name='Asthma')
    rosetta.synonymizer.synonymize(input_node)
    print(input_node.synonyms)
    results = ctd.disease_to_chemical(input_node)
    #Now, we're not returning the inferred ones.
    assert len(results) > 100
    for edge, node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'

def test_gene_to_drug_and_back(ctd):
    input_node = KNode('MESH:D003976', type=node_types.GENE, name='Diazinon')
    results = ctd.drug_to_gene(input_node)
    results = list(filter(lambda en: en[1].id == 'NCBIGENE:5243', results))
    for edge, node in results:
        assert node.type == node_types.GENE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
    dgedges = set([e.original_predicate.label for e, n in results])
    input_node_2 = KNode('NCBIGENE:5243', type=node_types.GENE, name='ABCB1')
    results = ctd.gene_to_drug(input_node_2)
    for edge, node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
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



def test_drug_to_gene_simple(ctd):
    input_node = KNode("MESH:D000068579", type=node_types.CHEMICAL_SUBSTANCE)
    results = ctd.drug_to_gene(input_node)
    for edge, node in results:
        assert node.type == node_types.GENE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
    result_ids = [node.id for edge, node in results]
    assert 'NCBIGENE:5743' in result_ids  # Cox2 for a cox2 inhibitor

def test_drug_to_gene_Huge(ctd):
    # Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("MESH:D014635", name="Valproic Acid", type=node_types.CHEMICAL_SUBSTANCE)
    results = ctd.drug_to_gene(input_node)
    #OK, this looks like a lot, but it's better than the 30000 we had before filtering.
    assert len(results) < 4000
    #print(results[0][0].original_predicate )
    #print(results[0][0].standard_predicate )
    #print(len(results))
    #assert 0

def test_drug_to_gene_synonym(ctd):
    # Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("DB:FakeID", type=node_types.CHEMICAL_SUBSTANCE)
    input_node.add_synonyms(set([LabeledID(identifier="MESH:D000068579", label="blah")]))
    results = ctd.drug_to_gene(input_node)
    for edge, node in results:
        assert node.type == node_types.GENE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
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
    for edge, n in results:
        assert edge.standard_predicate.identifier != 'GAMMA:0'
        if n.id == 'MESH:D004958':
            assert n.name == 'Estradiol'
    assert total == unique

def test_gene_to_drug_CASP3(ctd,rosetta):
    input_node = KNode("HGNC:1504", type=node_types.GENE)  # CASP3
    rosetta.synonymizer.synonymize(input_node)
    results = ctd.gene_to_drug(input_node)
    #See note in test_gene_to_drug_unique
    outputs = [(e.original_predicate, n.id) for e, n in results]
    total = len(outputs)
    unique = len(set(outputs))
    for e, n in results:
        assert e.standard_predicate.identifier != 'GAMMA:0'
        if (n.id == 'MESH:C059514'):
            print(e.standard_predicate.identifier)
    assert total == unique

def test_gene_to_drug_ACHE(ctd):
    input_node = KNode("NCBIGENE:43", type=node_types.GENE)  # ACHE
    results = ctd.gene_to_drug(input_node)
    #See note in test_gene_to_drug_unique
    outputs = [(e.original_predicate, n.id) for e, n in results]
    total = len(outputs)
    unique = len(set(outputs))
    for e, n in results:
        assert e.standard_predicate.identifier != 'GAMMA:0'
        if (n.id == 'MESH:D003976'):
            print(e)
    assert total == unique


def test_gene_to_drug_synonym(ctd):
    # Even though the main identifier is drugbank, CTD should find the right synonym in there somewhere.
    input_node = KNode("DB:FakeID", type=node_types.GENE)
    input_node.add_synonyms(set(["NCBIGene:5743"]))
    results = ctd.gene_to_drug(input_node)
    for e, node in results:
        assert e.standard_predicate.identifier != 'GAMMA:0'
        assert node.type == node_types.CHEMICAL_SUBSTANCE
    result_ids = [node.id for edge, node in results]
    assert 'MESH:D000068579' in result_ids  # Cox2 for a cox2 inhibitor

def test_gene_to_drug_BCL2(ctd,rosetta):
    input_node = KNode("HGNC:990", type=node_types.GENE, name="BCL2")
    rosetta.synonymizer.synonymize(input_node)
    results = ctd.gene_to_drug(input_node)
    assert len(results) > 0
    for edge,node in results:
        assert node.type == node_types.CHEMICAL_SUBSTANCE
        assert edge.standard_predicate.identifier != 'GAMMA:0'
        print(edge, edge.standard_predicate)


def test_chemical_to_gene_glutathione(ctd):
    input_node = KNode("MESH:D006861", type=node_types.CHEMICAL_SUBSTANCE)
    results = ctd.drug_to_gene(input_node)
    for edge, node in results:
        assert edge.standard_predicate.identifier != 'GAMMA:0'
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



