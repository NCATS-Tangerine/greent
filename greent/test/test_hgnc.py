import pytest
from greent.graph_components import KNode
from greent.services.hgnc import HGNC
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def hgnc():
    hgnc = HGNC(ServiceContext.create_context())
    return hgnc

def test_ncbi_to_uniprot(hgnc):
    hgnc = HGNC( ServiceContext.create_context() )
    input_knode = KNode( 'NCBIGENE:3815' , node_type = node_types.GENE )
    results = hgnc.ncbigene_to_uniprotkb( input_knode )
    assert(len(results) == 1)
    node = results[0][1]
    assert node.node_type == node_types.GENE
    assert Text.get_curie(node.identifier).upper() == 'UNIPROTKB'
    assert Text.un_curie(node.identifier) == 'P10721'

def test_synonym(hgnc):
    ncbigene = 'NCBIGENE:3815'
    syns = hgnc.get_synonyms(ncbigene)
    curies = [Text.get_curie(s).upper() for s in syns]
    for c in ['NCBIGENE','OMIM','UNIPROTKB','ENSEMBL','HGNC']:
        assert c in curies
