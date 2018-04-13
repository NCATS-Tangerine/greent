import pytest
from greent.graph_components import KNode
from greent.services.omnicorp import OmniCorp
from greent.servicecontext import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def omnicorpus():
    uberon = OmniCorp(ServiceContext.create_context())
    return uberon


def test_name(omnicorpus):
    cn ='CL:0000097'
    node = KNode(cn, node_type = node_types.CELL)
    oboid = omnicorpus.get_omni_identifier( node )
    assert oboid == 'http://purl.obolibrary.org/obo/CL_0000097'

def test_imatinib_asthma(omnicorpus):
    drug_node = KNode('CHEBI:45783', node_type = node_types.DRUG)
    disease_node = KNode('MONDO:0004979', node_type = node_types.DISEASE)
    pmids = omnicorpus.get_shared_pmids( drug_node, disease_node )
    assert len(pmids) > 0
    assert 'https://www.ncbi.nlm.nih.gov/pubmed/15374841' in pmids

def test_two_diesease(omnicorpus):
    disease1 = KNode('MONDO:0005090', node_type = node_types.DISEASE)
    disease2 = KNode('MONDO:0003425', node_type = node_types.DISEASE)
    pmids = omnicorpus.get_shared_pmids( disease1, disease2 )
    assert len(pmids) > 0


