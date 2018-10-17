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

def test_disease_to_go(uberon):
    #Make sure that for a variety of cell types we 1) get relationships going both directions and
    # that we map all predicates
    disease = KNode('MONDO:0009212',type=node_types.DISEASE, name="Congenital Factor X Deficiency")
    r = uberon.get_process_or_activity_by_disease( disease )
    for ri in r:
        assert ri[0].standard_predicate.identifier != 'GAMMA:0'
    assert len(r) > 50

def test_disease_to_go(uberon):
    #Make sure that for a variety of cell types we 1) get relationships going both directions and
    # that we map all predicates
    disease = KNode('HP:0012387',type=node_types.PHENOTYPIC_FEATURE, name="Bronchitis")
    r = uberon.get_process_or_activity_by_disease( disease )
    for ri in r:
        assert ri[0].standard_predicate.identifier != 'GAMMA:0'
    assert len(r) > 50

def test_cell_to_go(uberon):
    #Make sure that for a variety of cell types we 1) get relationships going both directions and
    # that we map all predicates
    for cell in ('CL:0000121','CL:0000513', 'CL:0000233','CL:0000097', 'CL:0002251'):
        foundsub = False
        foundobj = False
        r = uberon.get_process_or_activity_by_cell( KNode(cell, type=node_types.CELL, name="some cell"))
        for ri in r:
            assert ri[0].standard_predicate.identifier != 'GAMMA:0'
            if ri[0].source_id == ri[1].id:
                foundsub = True
            elif ri[0].target_id == ri[1].id:
                foundobj = True
        assert foundsub, foundobj

def test_go_to_cell(uberon):
    #Make sure that for a variety of cell types we 1) get relationships going both directions and
    # that we map all predicates
    go = 'GO:0045576'
    r = uberon.get_cell_by_process_or_activity( KNode(go, type=node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, name="some term"))
    foundsub = False
    foundobj = False
    for ri in r:
        assert ri[0].standard_predicate.identifier != 'GAMMA:0'
        if ri[0].source_id == ri[1].id:
            foundsub = True
        elif ri[0].target_id == ri[1].id:
            foundobj = True
    assert foundsub, foundobj


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
    assert len(results) >= 1
    idents = [ ke[1].id for ke in results ]
    assert 'UBERON:0002405' in idents

def test_anatomy_to_cell(uberon):
    k = KNode('UBERON:0002405', type=node_types.ANATOMICAL_ENTITY, name='Immune system')
    results = uberon.get_cell_by_anatomy_graph( k )
    #Mast cells are part of the immune system
    assert len(results) > 0
    identifiers = [result[1].id for result in results]
    for identifier in identifiers:
        assert identifier.startswith('CL:')
    assert 'CL:0000097' in identifiers

def test_anatomy_to_cell_upcast(uberon):
    k = KNode('CL:0000192', type=node_types.ANATOMICAL_ENTITY, name='Smooth Muscle Cell')
    results = uberon.get_cell_by_anatomy_graph( k )
    #There's no cell that's part of another cell?
    assert len(results) == 0

def test_pheno_to_anatomy_7354(uberon):
    #Arrhythmia occurs in...
    k = KNode('HP:0007354', type=node_types.PHENOTYPIC_FEATURE)
    results = uberon.get_anatomy_by_phenotype_graph( k )
    assert len(results) > 0


def test_pheno_to_anatomy(uberon):
    #Arrhythmia occurs in...
    k = KNode('HP:0011675', type=node_types.PHENOTYPIC_FEATURE)
    results = uberon.get_anatomy_by_phenotype_graph( k )
    #anatomical features
    ntypes = set([n.type for e,n in results])
    assert len(ntypes) == 1
    assert node_types.ANATOMICAL_ENTITY in ntypes
    identifiers = [n.id for e,n in results]
    assert 'UBERON:0000468' in identifiers #multicellular organism (yikes)
    assert 'UBERON:0004535' in identifiers #cardiovascular system
    assert 'UBERON:0000948' in identifiers #heart
    assert 'UBERON:0001981' in identifiers #blood vessel

def test_anat_to_pheno_odd(uberon):
    k = KNode('UBERON:3000111',type=node_types.ANATOMICAL_ENTITY)
    results = uberon.get_phenotype_by_anatomy_graph(k)
    if len(results) > 0:
        ntypes = set([n.type for e,n in results])
        assert len(ntypes) == 1
        assert node_types.PHENOTYPIC_FEATURE in ntypes

def test_anat_to_pheno(uberon):
    #Arrhythmia occurs in...
    k = KNode('UBERON:0000948', type=node_types.ANATOMICAL_ENTITY)
    results = uberon.get_phenotype_by_anatomy_graph( k )
    #phenos
    ntypes = set([n.type for e,n in results])
    assert len(ntypes) == 1
    assert node_types.PHENOTYPIC_FEATURE in ntypes
    identifiers = [n.id for e,n in results]
#    for e,n in results:
#        print( n.id, n.name )
    assert 'HP:0001750' in identifiers #single ventricle
    assert 'HP:0001644' in identifiers #dilated cardiomyopathy

def test_non_HP_pheno_to_anatomy(uberon):
    #Arrhythmia occurs in...
    k = KNode('xx:0011675', type=node_types.PHENOTYPIC_FEATURE)
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
