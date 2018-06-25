import pytest
from greent.graph_components import LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta


def test_contributes_to(rosetta):
    concept_model = rosetta.type_graph.concept_model
    predicate_id = LabeledID('RO:0003302','causes_or_contributes_to')
    standard = concept_model.standardize_relationship(predicate_id)
    assert standard.identifier == 'RO:0002326'
    assert standard.label == 'contributes_to'


