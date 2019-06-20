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

def test_drugcentral(mychem):
    node = KNode('CHEMBL:CHEMBL118', type=node_types.CHEMICAL_SUBSTANCE, name='Celecoxib') #Celecoxib
    results = mychem.get_drugcentral(node)
    found1 = False
    found2 = False
    for e,n in results:
        if n.id == 'UMLS:C0007222':
            found1 = True
            assert e.original_predicate.label == 'contraindication'
        if n.id == 'UMLS:C0003873':
            found2 = True
            assert e.original_predicate.label == 'treats'
        assert e.provided_by == 'mychem.get_drugcentral'
    assert found1
    assert found2

def test_glyburide(rosetta,mychem):
    node = KNode('CHEBI:5441', type=node_types.CHEMICAL_SUBSTANCE, name='glyburide')
    rosetta.synonymizer.synonymize(node)
    results = mychem.get_drugcentral(node)
    found1 = False
    found2 = False
    for e,n in results:
        print(e.original_predicate.label, n)
#        if n.id == 'UMLS:C0007222':
#            found1 = True
#            assert e.original_predicate.label == 'contraindication'
#        if n.id == 'UMLS:C0003873':
#            found2 = True
#            assert e.original_predicate.label == 'treats'
#        assert e.provided_by == 'mychem.get_drugcentral'
    assert found1
    assert found2

def test_glyburide_aeolus(rosetta,mychem):
    node = KNode('CHEBI:5441', type=node_types.CHEMICAL_SUBSTANCE, name='glyburide')
    rosetta.synonymizer.synonymize(node)
    results = mychem.get_adverse_events(node)
    indications=['diabetes mellitus', ]
    for e,n in results:
        print(e.original_predicate.label, n.name)

def test_drug_adverse_events(mychem):
    node = KNode('CHEMBL:CHEMBL1508', type=node_types.CHEMICAL_SUBSTANCE) #Escitalopram
    results = mychem.get_adverse_events(node)
    #for e,n in results:
    #    print(n)
    assert len(results) > 0

def test_atorvastatin(mychem):
    node = KNode('CHEMBL:CHEMBL1487', type=node_types.CHEMICAL_SUBSTANCE) #Escitalopram
    results = mychem.get_adverse_events(node)
    assert len(results) > 0

def x_test_event_to_drug(mychem):
    node = KNode('MONDO:0002050', type=node_types.DISEASE, name='Mental Depression')
    node.add_synonyms( set( [LabeledID(identifier='MedDRA:10002855', label='Depression')]))
    results = mychem.get_drug_from_adverse_events(node)
    assert len(results) > 0

def x_test_event_to_drug(mychem):
    node = KNode('HP:0002018', type=node_types.PHENOTYPIC_FEATURE, name='Nausea')
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
    results = func(KNode('CHEMBL:CHEMBL1508', type=node_types.CHEMICAL_SUBSTANCE))
    assert len(results) > 0


def test_drug_gene(mychem):
    node = KNode('DRUGBANK:DB00802', type=node_types.CHEMICAL_SUBSTANCE) # Alfentanyl
    results = mychem.get_gene_from_drug(node)
    assert len(results) > 0