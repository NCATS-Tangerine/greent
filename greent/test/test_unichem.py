import pytest
from greent.graph_components import KNode
from greent.services.unichem import UniChem
from greent.service import ServiceContext
from greent import node_types
#from greent.util import Text

@pytest.fixture(scope='module')
def unichem():
    unichem = UniChem(ServiceContext.create_context())
    return unichem

def test_chembl_input (unichem):
    q = UniChem (ServiceContext.create_context ())
    synonyms = unichem.get_synonyms("CHEMBL:CHEMBL12")
    assert len(synonyms) == 4
    assert 'CHEMBL:CHEMBL12' in synonyms #Returns input also
    assert 'DRUGBANK:DB00829' in synonyms
    assert 'CHEBI:49575' in synonyms
    assert 'PUBCHEM:3016' in synonyms

def test_chebi_input (unichem):
    q = UniChem (ServiceContext.create_context ())
    synonyms = unichem.get_synonyms("CHEBI:49575")
    assert len(synonyms) == 4
    assert 'CHEMBL:CHEMBL12' in synonyms
    assert 'DRUGBANK:DB00829' in synonyms
    assert 'CHEBI:49575' in synonyms
    assert 'PUBCHEM:3016' in synonyms

def test_unknown_curie(unichem):
    q = UniChem (ServiceContext.create_context ())
    synonyms = unichem.get_synonyms("DruggyDrug:49575")
    assert len(synonyms) == 0

