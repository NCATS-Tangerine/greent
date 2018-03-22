import pytest
from greent.graph_components import KNode
from greent.services.pharos import Pharos
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def pharos():
    pharos = Pharos(ServiceContext.create_context())
    return pharos

def test_string_to_info(pharos):
    r = pharos.drugname_string_to_pharos_info('CELECOXIB')
    assert len(r) == 1
    assert r[0][0] == 'CHEMBL:CHEMBL118' #first result is a tuple (curie,name)

def test_string_to_info_wackycap(pharos):
    r = pharos.drugname_string_to_pharos_info('CeLEcOXIB')
    assert len(r) == 1
    assert r[0][0] == 'CHEMBL:CHEMBL118' #first result is a tuple (curie,name)

def test_drug_get_gene(pharos):
    #pharos should find chembl in the synonyms
    node = KNode('DB:FakeyName',node_type = node_types.DRUG)
    node.add_synonyms(['CHEMBL:CHEMBL118'])
    results = pharos.drug_get_gene(node)
    #we get results
    assert len(results) > 0
    #They are gene nodes:
    ntypes = set([n.node_type for e,n in results])
    assert node_types.GENE in ntypes
    assert len(ntypes) == 1
    #All of the ids should be HGNC
    identifiers = [n.identifier for e,n in results]
    prefixes = set([ Text.get_curie(i) for i in identifiers])
    assert 'HGNC' in prefixes
    assert len(prefixes) == 1
    #PTGS2 (COX2) (HGNC:9605) should be in there
    assert 'HGNC:9605' in identifiers

def test_gene_get_drug(pharos):
    #Pharos will find DOIDs or whatever it needs in the synonyms
    node = KNode('MONDO:XXXX', node_types.DISEASE)
    node.add_synonyms(['DOID:4325']) #Ebola
    results = pharos.disease_get_gene(node)
    #we get results
    assert len(results) > 0
    #They are gene nodes:
    ntypes = set([n.node_type for e,n in results])
    assert node_types.GENE in ntypes
    assert len(ntypes) == 1
    #All of the ids should be HGNC
    identifiers = [n.identifier for e,n in results]
    prefixes = set([ Text.get_curie(i) for i in identifiers])
    assert 'HGNC' in prefixes
    assert len(prefixes) == 1
    #NPC1 (HGNC:7897) should be in there
    assert 'HGNC:7897' in identifiers

