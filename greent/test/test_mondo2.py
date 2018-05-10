import pytest
from greent.graph_components import KNode
from greent.ontologies.mondo2 import Mondo2
from greent.servicecontext import ServiceContext
from greent import node_types

@pytest.fixture(scope='module')
def mondo2():
    return Mondo2(ServiceContext.create_context())

def test_huntington_is_genetic(mondo2):
    huntington = KNode('OMIM:143100',node_types.DISEASE)
    assert mondo2.is_genetic_disease(huntington)

def test_lookup(mondo2):
    terms1=mondo2.search('Huntington Disease')
    terms2=mondo2.search("Huntington's Chorea")
    assert len(terms1) == len(terms2) == 1
    assert terms1[0] == terms2[0] == 'MONDO:0007739'

def test_xrefs(mondo2): 
    xrefs = mondo2.get_xrefs ('MONDO:0005737')    
    xref_ids = [ x['id'] for x in xrefs ]
    print (xref_ids)
    for i in [ "DOID:4325", "EFO:0007243", "ICD10:A98.4", "MedDRA:10014071", "MESH:D019142", "NCIT:C36171", "Orphanet:319218", "SCTID:37109004", "UMLS:C0282687" ]:
        assert i in xref_ids

def test_get_mondoId(mondo2):
    mondoids = mondo2.get_mondo_id('DOID:9008')
    assert mondoids[0]== 'MONDO:0011849'

def test_get_doid(mondo2):
    mondoid = 'MONDO:0009757'
    doids = mondo2.mondo_get_doid(mondoid)
    assert len(doids) == 0
