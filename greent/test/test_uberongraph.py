import pytest
from greent.graph_components import KNode
from greent.services.uberongraph import UberonGraphKS
from greent.servicecontext import ServiceContext
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def uberon(rosetta):
    uberon = rosetta.core.uberongraph
    return uberon

def test_name(uberon):
    cn ='CL:0000097'
    results = uberon.cell_get_cellname( cn )
    assert len(results) == 1
    assert results[0]['cellLabel'] == 'mast cell'

def test_cell_to_anatomy_super(uberon):
    k = KNode('CL:0002251', type=node_types.CELL, name='epithelial cell of the alimentary canal')
    results = uberon.get_anatomy_by_cell_graph( k )
    #Should get back digestive system UBERON:0001007
    assert len(results) > 0
    idents = [ ke[1].id for ke in results ]
    print(idents)
    assert 'UBERON:0001007' in idents


def test_cell_to_anatomy(uberon):
    k = KNode('CL:0000097', type=node_types.CELL)
    results = uberon.get_anatomy_by_cell_graph( k )
    #Mast cells are part of the immune system
    assert len(results) == 1
    node = results[0][1]
    assert node.type  == node_types.ANATOMY
    assert node.id == 'UBERON:0002405'

def test_anatomy_to_cell(uberon):
    k = KNode('UBERON:0002405', type=node_types.ANATOMY, name='Immune system')
    results = uberon.get_cell_by_anatomy_graph( k )
    #Mast cells are part of the immune system
    assert len(results) > 0
    identifiers = [result[1].id for result in results]
    for identifier in identifiers:
        assert identifier.startswith('CL:')
    assert 'CL:0000097' in identifiers

def test_anatomy_to_cell_upcast(uberon):
    k = KNode('CL:0000192', type=node_types.ANATOMY, name='Smooth Muscle Cell')
    results = uberon.get_cell_by_anatomy_graph( k )
    #There's no cell that's part of another cell?
    assert len(results) == 0

def test_pheno_to_anatomy(uberon):
    #Arrhythmia occurs in...
    k = KNode('HP:0011675', type=node_types.PHENOTYPE)
    results = uberon.get_anatomy_by_phenotype_graph( k )
    #anatomical features
    ntypes = set([n.type for e,n in results])
    assert len(ntypes) == 1
    assert node_types.ANATOMY in ntypes
    identifiers = [n.id for e,n in results]
    assert 'UBERON:0000468' in identifiers #multicellular organism (yikes)
    assert 'UBERON:0004535' in identifiers #cardiovascular system
    assert 'UBERON:0000948' in identifiers #heart
    assert 'UBERON:0001981' in identifiers #blood vessel

def test_anat_to_pheno(uberon):
    #Arrhythmia occurs in...
    k = KNode('UBERON:0000948', type=node_types.ANATOMY)
    results = uberon.get_phenotype_by_anatomy_graph( k )
    #phenos
    ntypes = set([n.type for e,n in results])
    assert len(ntypes) == 1
    assert node_types.PHENOTYPE in ntypes
    identifiers = [n.id for e,n in results]
#    for e,n in results:
#        print( n.id, n.name )
    assert 'HP:0001750' in identifiers #single ventricle
    assert 'HP:0001644' in identifiers #dilated cardiomyopathy

def test_non_HP_pheno_to_anatomy(uberon):
    #Arrhythmia occurs in...
    k = KNode('xx:0011675', type=node_types.PHENOTYPE)
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
