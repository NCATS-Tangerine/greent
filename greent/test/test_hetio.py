import pytest
from greent.graph_components import KNode
#from greent.services.biolink import Biolink
#from greent.ontologies.mondo import Mondo
#from greent.servicecontext import ServiceContext
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta


@pytest.fixture()
def hetio(rosetta):
    hetio = rosetta.core.hetio
    return hetio


def test_gene_to_anatomy(hetio):
    relations = hetio.gene_to_anatomy(KNode('NCBIGENE:83752',node_types.GENE))
    assert len(relations) < 20 and len(relations) > 10
    identifiers = [node.identifier for r,node in relations]
    #everything should be UBERON ids
    for ident in identifiers:
        assert Text.get_curie(ident) == 'UBERON'
    assert 'UBERON:0001007' in identifiers

def test_gene_to_disease(hetio):
    #KRT7 associated with bile duct cancer?
    relations = hetio.gene_to_disease(KNode('NCBIGENE:3855',node_types.GENE))
    assert len(relations) < 20 and len(relations) > 10
    identifiers = [node.identifier for r,node in relations]
    #everything should be UBERON ids
    for ident in identifiers:
        assert Text.get_curie(ident) == 'DOID'
    assert 'DOID:4606' in identifiers

def test_disease_to_symptom(hetio):
    #Crohn's disease has associated Skin Manifesations?
    relations = hetio.disease_to_phenotype(KNode('DOID:8778',node_types.DISEASE))
    identifiers = [node.identifier for r,node in relations]
    #everything should be UBERON ids
    for ident in identifiers:
        assert Text.get_curie(ident) == 'MESH'
    assert 'MESH:D012877' in identifiers
