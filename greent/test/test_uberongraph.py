import pytest
from greent.graph_components import KNode
from greent.services.uberongraph import UberonGraphKS
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def uberon():
    uberon = UberonGraphKS(ServiceContext.create_context())
    return uberon


def test_name(uberon):
    cn ='CL:0000097'
    results = uberon.cell_get_cellname( cn )
    assert len(results) == 1
    assert results[0]['cellLabel'] == 'mast cell'

def test_cell_to_anatomy(uberon):
    k = KNode('CL:0000097',node_types.CELL)
    results = uberon.get_anatomy_by_cell_graph( k )
    #Mast cells are part of the immune system
    assert len(results) == 1
    node = results[0][1]
    assert node.node_type  == node_types.ANATOMY
    assert node.identifier == 'UBERON:0002405'

def test_pheno_to_anatomy(uberon):
    #Arrhythmia occurs in...
    k = KNode('HP:0011675',node_types.PHENOTYPE)
    results = uberon.get_anatomy_by_phenotype_graph( k )
    #anatomical features
    ntypes = set([n.node_type for e,n in results])
    assert len(ntypes) == 1
    assert node_types.ANATOMY in ntypes
    identifiers = [n.identifier for e,n in results]
    assert 'UBERON:0000468' in identifiers #multicellular organism (yikes)
    assert 'UBERON:0004535' in identifiers #cardiovascular system
    assert 'UBERON:0000948' in identifiers #heart
    assert 'UBERON:0001981' in identifiers #blood vessel

def test_non_HP_pheno_to_anatomy(uberon):
    #Arrhythmia occurs in...
    k = KNode('xx:0011675',node_types.PHENOTYPE)
    results = uberon.get_anatomy_by_phenotype_graph( k )
    assert len(results) == 0

def test_parts(uberon):
    uk = UberonGraphKS(ServiceContext.create_context ())
    results = uberon.get_anatomy_parts('UBERON:0004535')
    #What are the parts of the cardiovascular system?
    #Note that we don't use this atm, it's just here as an example
    curies = [x['curie'] for x in results]
    assert 'UBERON:0000948' in curies #heart
    assert 'UBERON:0001981' in curies #blood vessel
