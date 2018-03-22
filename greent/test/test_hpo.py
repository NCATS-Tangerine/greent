import pytest
from greent.ontologies.hpo import HPO
from greent.service import ServiceContext

@pytest.fixture(scope='module')
def hpo():
    hpo = HPO(ServiceContext.create_context())
    return hpo

def test_lookup(hpo):
    terms1=hpo.search('Arrhythmias, Cardiac')
    terms2=hpo.search('CARDIAC ARRHYTHMIAS')
    assert len(terms1) == len(terms2) == 1
    assert terms1[0] == terms2[0] == 'HP:0011675'

