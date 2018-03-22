import pytest
from greent.graph_components import KNode
from greent.ontologies.mondo import Mondo
from greent.service import ServiceContext
from greent import node_types

@pytest.fixture(scope='module')
def mondo():
    mondo = Mondo(ServiceContext.create_context())
    return mondo

def test_huntington_is_genetic(mondo):
    huntington = KNode('OMIM:143100',node_types.DISEASE)
    assert mondo.is_genetic_disease(huntington)

def test_lookup(mondo):
    terms1=mondo.search('Huntington Disease')
    terms2=mondo.search("Huntington's Chorea")
    assert len(terms1) == len(terms2) == 1
    assert terms1[0] == terms2[0] == 'MONDO:0007739'

