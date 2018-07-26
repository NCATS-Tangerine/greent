import pytest
from greent.graph_components import KNode,LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta
from greent.synonymizers.disease_synonymizer import synonymize


@pytest.fixture()
def mychem(rosetta):
    mychem = rosetta.core.mychem
    return mychem

def test_drug_adverse_events(mychem):
    node = KNode('CHEMBL:CHEMBL1508', type=node_types.DRUG) #Escitalopram
    results = mychem.get_adverse_events(node)
    for e,n in results:
        print(n)
    assert False
    assert len(results) > 0

def test_event_to_drug(mychem):
    node = KNode('MONDO:0002050', type=node_types.DISEASE, name='Mental Depression')
    node.add_synonyms( set( [LabeledID(identifier='MedDRA:10002855', label='Depression')]))
    results = mychem.get_drug_from_adverse_events(node)
    assert len(results) > 0

def test_event_to_drug(mychem):
    node = KNode('HP:0002018', type=node_types.PHENOTYPE, name='Nausea')
    node.add_synonyms( set( [LabeledID(identifier='MedDRA:10028813', label='Nausea')]))
    results = mychem.get_drug_from_adverse_events(node)
    assert len(results) > 0


#This test is accurate, but pheno filter is slow, so it doesn't make a good test
def x_test_with_pheno_filter(rosetta):
    """This will usually get called with a phenotype filter"""
    fname='caster.output_filter(mychem~get_adverse_events,phenotypic_feature,typecheck~is_phenotypic_feature)'
    func = rosetta.get_ops(fname)
    assert func is not None
    #Escitalopram
    results = func(KNode('CHEMBL:CHEMBL1508', type=node_types.DRUG))
    assert len(results) > 0


