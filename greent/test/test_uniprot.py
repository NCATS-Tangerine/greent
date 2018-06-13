import pytest
from greent.services.uniprot import UniProt
from greent.servicecontext import ServiceContext
from greent.util import Text

@pytest.fixture(scope='module')
def uniprot():
    uniprot = UniProt(ServiceContext.create_context())
    return uniprot

def test_uniprot_both(uniprot):
    uni = 'UniProtKB:A0A024QZH5'
    results = uniprot.get_synonyms(uni)
    assert len(results) == 1
    assert results[0] == 'NCBIGene:56848'

def test_uniprot_fail(uniprot):
    uni = 'UniProtKB:A0A024QZH5'
    ncbis = uniprot.uniprot_2_ncbi(uni)
    assert len(ncbis) == 1
    hgncs = uniprot.uniprot_2_hgnc(uni)
    assert len(hgncs) == 0
    assert ncbis[0] == 'NCBIGene:56848'

def test_uniprot(uniprot):
    uni = 'UniProtKB:A0A096LNX8'
    hgncs = uniprot.uniprot_2_hgnc(uni)
    assert len(hgncs) == 1
    assert hgncs[0] == 'HGNC:19869'
    #curies = [Text.get_curie(s.identifier).upper() for s in syns]
    #for c in ['NCBIGENE','OMIM','UNIPROTKB','ENSEMBL','HGNC']:
    #    assert c in curies

#def test_uniprot(hgnc):
#    uniprot='UniProtKB:Q96RI1'
#    syns = [s.identifier for s  in hgnc.get_synonyms(uniprot) ]
#    assert 'HGNC:7967' in syns
#
