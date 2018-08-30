import pytest
from greent import node_types
from greent.conftest import rosetta


def test_parents(rosetta):
    concept_model = rosetta.type_graph.concept_model
    parent = concept_model.get_parent(node_types.DISEASE)
    assert parent == node_types.DISEASE_OR_PHENOTYPIC_FEATURE

def test_get_children(rosetta):
    concept_model = rosetta.type_graph.concept_model
    children = concept_model.get_children(node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    assert len(children) == 2
    assert node_types.DISEASE in children
    assert node_types.PHENOTYPIC_FEATURE in children

def test_genetic_condition(rosetta):
    concept_model = rosetta.type_graph.concept_model
    children = concept_model.get_children(node_types.DISEASE)
    assert len(children) == 1
    assert node_types.GENETIC_CONDITION in children

def test_DOPF(rosetta):
    concept_model = rosetta.type_graph.concept_model
    parent = concept_model.get_parent(node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    assert parent == node_types.BIOLOGICAL_ENTITY

def test_named_thing_to_disease(rosetta):
    concept_model = rosetta.type_graph.concept_model
    children = concept_model.get_children(node_types.NAMED_THING)
    assert node_types.BIOLOGICAL_ENTITY in children
    children = concept_model.get_children(node_types.BIOLOGICAL_ENTITY)
    assert node_types.DISEASE_OR_PHENOTYPIC_FEATURE in children
    children = concept_model.get_children(node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    assert node_types.DISEASE in children

def test_metabolite_children(rosetta):
    concept_model = rosetta.type_graph.concept_model
    children = concept_model.get_children(node_types.METABOLITE)
    assert len(children) == 0

def test_id_prefixes(rosetta):
    concept_model = rosetta.type_graph.concept_model
    assert concept_model.get(node_types.CHEMICAL_SUBSTANCE).id_prefixes == concept_model.get(node_types.DRUG).id_prefixes
    assert concept_model.get(node_types.CHEMICAL_SUBSTANCE).id_prefixes == concept_model.get(node_types.METABOLITE).id_prefixes
    assert concept_model.get(node_types.GENE).id_prefixes == ['HGNC','NCBIGENE','ENSEMBL','UniProtKB','EC','RNAcentral','MGI','ZFIN']
    assert concept_model.get(node_types.CELL).id_prefixes == ['CL','UMLS']
    #anatomical entity has its own prefixes uberon & umls, so they come first
    # after that, it's made up of cellular component( go ), cell (cl, umls) and gross anatomical structure (uberon, po, fao)
    # these get interleaved & deduped
    assert concept_model.get(node_types.ANATOMICAL_ENTITY).id_prefixes == ['UBERON','UMLS','GO','CL','PO','FAO']
    dop = concept_model.get(node_types.DISEASE_OR_PHENOTYPIC_FEATURE).id_prefixes[:2]
    assert 'MONDO' in dop
    assert 'HP' in dop
    assert concept_model.get(node_types.BIOLOGICAL_PROCESS).id_prefixes[0] == 'GO'
    assert concept_model.get(node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY).id_prefixes[0] == 'GO'

