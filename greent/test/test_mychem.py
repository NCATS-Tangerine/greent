import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta
from greent.synonymizers.disease_synonymizer import synonymize


@pytest.fixture()
def mychem(rosetta):
    mychem = rosetta.core.mychem
    return mychem

def test_drug_adverse_events(mychem):
    node = KNode('CHEMBL:CHEMBL1508',node_types.DRUG) #Escitalopram
    results = mychem.get_adverse_events(node)
    assert len(results) > 0

#This test is accurate, but pheno filter is slow, so it doesn't make a good test
def x_test_with_pheno_filter(rosetta):
    """This will usually get called with a phenotype filter"""
    fname='caster.output_filter(mychem~get_adverse_events,phenotypic_feature,typecheck~is_phenotypic_feature)'
    func = rosetta.get_ops(fname)
    assert func is not None
    #Escitalopram
    results = func(KNode('CHEMBL:CHEMBL1508',node_types.DRUG))
    assert len(results) > 0


