import pytest
from greent.graph_components import KNode
from greent.services.biolink import Biolink
from greent.ontologies.mondo import Mondo
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text

@pytest.fixture(scope='module')
def biolink():
    biolink = Biolink(ServiceContext.create_context())
    return biolink

@pytest.fixture(scope='module')
def mondo():
    checker = Mondo(ServiceContext.create_context())
    return checker

def test_gene_to_disease(biolink):
    """What do we get back for HBB"""
    relations = biolink.gene_get_disease(KNode('HGNC:4827',node_types.GENE))
    assert len(relations) > 20 and len(relations) < 40
    identifiers = [node.identifier for r,node in relations]
    #everthing should be MONDO ids
    for ident in identifiers:
        assert Text.get_curie(ident) == 'MONDO'
    #Sickle cell should be in there.
    assert 'MONDO:0011382' in identifiers

def test_gc(biolink,mondo):
    gene = KNode('HGNC:4827', node_type=node_types.GENE)
    original_results = biolink.gene_get_disease(gene)
    gc_results = biolink.gene_get_genetic_condition(gene)
    disease_identifiers = set([node.identifier for r,node in original_results])
    for e, k in gc_results:
        assert k.identifier in disease_identifiers
        assert k.node_type == node_types.GENETIC_CONDITION

def test_gene_to_process(biolink):
    KIT_protein = KNode('UniProtKB:P10721', node_types.GENE)
    results = biolink.gene_get_process(KIT_protein)
    for ke, kn in results:
        assert kn.node_type == node_types.PROCESS
        assert Text.get_curie(kn.identifier) == "GO"

def test_disease_to_phenotypes(biolink):
    asthma = KNode('DOID:2841', node_types.DISEASE)
    results = biolink.disease_get_phenotype(asthma)
    assert len(results) > 90 and len(results) < 110
    identifiers = [node.identifier for r,node in results]
    #everthing should be MONDO ids
    for ident in identifiers:
        assert Text.get_curie(ident) == 'HP'
    #acute severe asthma should be in there.
    assert 'HP:0012653' in identifiers

def test_pathways(biolink):
    gene_id = 'HGNC:5013'
    gene = KNode(gene_id, node_type=node_types.GENE)
    results = biolink.gene_get_pathways(gene)
    for e, k in results:
        print( k.identifier )
        assert k.node_type == node_types.PATHWAY
        presults = biolink.pathway_get_gene(k)
        for pe,pk in presults:
            assert pk.node_type == node_types.GENE
        gene_ids = [ pk.identifier for pe,pk in presults ]
        #TODO: This doesn't work because we're not handling paging correctly
        #assert gene_id in gene_ids
