import pytest
from greent.ontologies.hpo2 import HPO2
from greent.service import ServiceContext

@pytest.fixture(scope='module')
def hpo2():
    return HPO2(ServiceContext.create_context())

def test_lookup(hpo2):
    terms1=hpo2.search('Arrhythmias, Cardiac')
    terms2=hpo2.search('CARDIAC ARRHYTHMIAS')
    assert len(terms1) == len(terms2) == 1
    assert terms1[0] == terms2[0] == 'HP:0011675'

