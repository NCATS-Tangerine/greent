import pytest
from greent.graph_components import KNode
from greent.services.oxo import OXO
from greent.service import ServiceContext
from greent import node_types
#from greent.util import Text

@pytest.fixture(scope='module')
def oxo():
    oxo = OXO(ServiceContext.create_context())
    return oxo

def test_prefixes(oxo):
    assert oxo.is_valid_curie_prefix("EFO")
    assert not oxo.is_valid_curie_prefix("NCBIGENE")
    assert not  oxo.is_valid_curie_prefix("NCBIGene")
    assert oxo.is_valid_curie_prefix("DOID")
    #do I care about case?  Jim says curie are case sensitive...
    #assert oxo.is_valid_curie_prefix("DoiD")
    assert oxo.is_valid_curie_prefix("MESH")
    assert oxo.is_valid_curie_prefix("MeSH")
    assert not oxo.is_valid_curie_prefix("dummy")
    assert not oxo.is_valid_curie_prefix("MONDO")

def test_synonyms(oxo):
    curieset = oxo.get_synonymous_curies('EFO:0000764')
    #A bunch of stuff comes back. We'll spot check a few
    assert 'MeSH:D015658' in curieset
    assert 'DOID:526' in curieset
    assert 'UMLS:C1858709' in curieset

