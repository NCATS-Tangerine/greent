import pytest
from greent.graph_components import KNode
from greent.ontologies.mondo2 import Mondo2
from greent.servicecontext import ServiceContext
from greent import node_types

@pytest.fixture(scope='module')
def mondo():
    mondo = Mondo2(ServiceContext.create_context())
    return mondo

def test_huntington_is_genetic(mondo):
    huntington = KNode('OMIM:143100', type=node_types.DISEASE)
    assert mondo.is_genetic_disease(huntington)

def test_lookup(mondo):
    terms1=mondo.search('Huntington Disease')
    terms2=mondo.search("Huntington's Chorea")
    assert len(terms1) == len(terms2) == 1
    assert terms1[0] == terms2[0] == 'MONDO:0007739'

def test_exact_matches(mondo):
    ids = set(mondo.get_exact_matches('MONDO:0005737'))
    goods = set(['DOID:4325', 'meddra:10014071', 'mesh:D019142', 'snomedct:37109004', 'NCIT:C36171', 'Orphanet:319218', 'umls:C0282687'])  
    assert ids == goods
