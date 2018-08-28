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

