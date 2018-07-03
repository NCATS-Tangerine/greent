import pytest
from greent.graph_components import KNode
from greent.servicecontext import ServiceContext
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def omnicorpus(rosetta):
    return rosetta.core.omnicorp

def test_connect(omnicorpus):
    assert True

def test_imatinib_asthma(omnicorpus):
    drug_node = KNode('CHEBI:45783', node_type = node_types.DRUG)
    disease_node = KNode('MONDO:0004979', node_type = node_types.DISEASE)
    pmids = omnicorpus.get_shared_pmids( drug_node, disease_node )
    assert len(pmids) > 0
    assert 'PMID:15374841' in pmids

def test_two_disease(omnicorpus):
    disease1 = KNode('MONDO:0005090', node_type = node_types.DISEASE)
    disease2 = KNode('MONDO:0003425', node_type = node_types.DISEASE)
    pmids = omnicorpus.get_shared_pmids( disease1, disease2 )
    assert len(pmids) > 0

'''Don't care right now
def test_list(omnicorpus):
    node = KNode('CL:0000097', node_type = node_types.CELL)
    disease_node = KNode('MONDO:0004979', node_type = node_types.DISEASE)
    drug_node = KNode('CHEBI:45783', node_type = node_types.DRUG)
    nodes = [ node, disease_node, drug_node ]
    results = omnicorpus.get_all_shared_pmids( nodes )
    assert len(results) == 3
    ids = set()
    for n1,n2 in results:
        ids.add(n1.identifier)
        ids.add(n2.identifier)
        assert len(results[(n1,n2)]) > 0
    assert len(ids) == 3
    assert 'CL:0000097' in ids
    assert 'MONDO:0004979' in ids
    assert 'CHEBI:45783' in ids
'''

def test_list_returns_zero(omnicorpus):
    disease_node = KNode('UBERON:0013694', node_type = node_types.ANATOMY)
    go_node = KNode('GO:0045892', node_type = node_types.PROCESS)
    results = omnicorpus.get_shared_pmids( disease_node, go_node )
    assert len(results) == 0

#Need 1 more tests: What if we have a CHEMBL as our main id?  It will fail w/o looking for a CHEBI (bad)

def test_pmid_count(omnicorpus):
    uberon_node = KNode('UBERON:0013694', node_type = node_types.ANATOMY)
    n = omnicorpus.count_pmids(uberon_node)
    #Checked by hand in the database
    assert n == 2058
