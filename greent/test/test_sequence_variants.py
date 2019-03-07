import pytest
from greent.graph_components import KNode,LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def myvariant(rosetta):
    myvariant = rosetta.core.myvariant
    return myvariant

@pytest.fixture()
def clingen(rosetta):
    clingen = rosetta.core.clingen
    return clingen

@pytest.fixture()
def gwascatalog(rosetta):
    gwascatalog = rosetta.core.gwascatalog
    return gwascatalog

@pytest.fixture()
def biolink(rosetta):
    biolink = rosetta.core.biolink
    return biolink

def test_synonymization(rosetta, clingen):
    variant_node = KNode('CAID:CA128085', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'HGVS:NC_000012.12:g.111803962G>A' in variant_node.get_synonyms_by_prefix('HGVS')
    assert 'CLINVARVARIANT:18390' in variant_node.get_synonyms_by_prefix('CLINVARVARIANT')
    assert 'DBSNP:rs671' in variant_node.get_synonyms_by_prefix('DBSNP')

    variant_node = KNode('DBSNP:rs369602258', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA321211' in variant_node.get_synonyms_by_prefix('CAID')
    assert 'MYVARIANT_HG38:chr11:g.68032291C>T' in variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')
    # TODO: it should have these as well - 
    # assert 'CAID:CA6146346' in variant_node.get_synonyms_by_prefix('CAID')
    # assert 'MYVARIANT_HG38:chr11:g.68032291C>G' in variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')

    variant_node = KNode('HGVS:NC_000023.9:g.32317682G>A', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA267021' in variant_node.get_synonyms_by_prefix('CAID')
    assert 'MYVARIANT_HG38:chrX:g.32389644G>A' in variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')
    assert 'CLINVARVARIANT:94623' in variant_node.get_synonyms_by_prefix('CLINVARVARIANT')

    variant_node = KNode('CLINVARVARIANT:18390', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA128085' in variant_node.get_synonyms_by_prefix('CAID')

    #variant_node = KNode('MYVARIANT_HG19:chr11:g.67799758C>G', type=node_types.SEQUENCE_VARIANT)
    #rosetta.synonymizer.synonymize(variant_node)
    #assert 'CAID:CA6146346' in variant_node.get_synonyms_by_prefix('CAID') 
    #assert 'DBSNP:rs369602258' in variant_node.get_synonyms_by_prefix('DBSNP')
    #assert 'HGVS:NC_000011.10:g.68032291C>G' in variant_node.get_synonyms_by_prefix('HGVS')
    #assert 'HGVS:CM000673.2:g.68032291C>G' in variant_node.get_synonyms_by_prefix('HGVS')

    #variant_node = KNode('MYVARIANT_HG38:chr11:g.68032291C>G', type=node_types.SEQUENCE_VARIANT)
    #rosetta.synonymizer.synonymize(variant_node)
    #assert 'CAID:CA6146346' in variant_node.get_synonyms_by_prefix('CAID')
    #assert 'DBSNP:rs369602258' in variant_node.get_synonyms_by_prefix('DBSNP')

    hgvs_ids = ['NC_000011.10:g.68032291C>G', 'NC_000023.9:g.32317682G>A', 'NC_000017.10:g.43009069G>C', 'NC_000017.10:g.43009127delG']
    batch_synonyms = clingen.get_batch_of_synonyms(hgvs_ids)
  
    synonyms_1 = [identifier for identifier, label in batch_synonyms['HGVS:NC_000023.9:g.32317682G>A']]
    assert 'CAID:CA267021' in synonyms_1
  
    synonyms_2 = [identifier for identifier, label in batch_synonyms['HGVS:NC_000011.10:g.68032291C>G']]
    assert 'DBSNP:rs369602258' in synonyms_2

def test_sequence_variant_to_gene(myvariant):

    variant_node = KNode('MYVARIANT_HG19:chr7:g.55241707G>T', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:3236' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_missense_variant_of' in plabels
    assert 'is_nearby_variant_of' in plabels
    pids = [p.identifier for p in predicates]
    assert 'SO:0001583' in pids
    assert 'GAMMA:0000102' in pids

    variant_node = KNode('MYVARIANT_HG38:chr11:g.68032291C>G', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:7715' in identifiers
    assert 'HGNC:41796' in identifiers
    assert 'HGNC:410' in identifiers

    # this one has unmapped annotations that should be skipped
    variant_node4 = KNode('MYVARIANT_HG19:chr1:g.145440440C>T', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node4)
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    pids = [p.identifier for p in predicates]
    assert 'GAMMA:0' not in pids

    variant_node5 = KNode('MYVARIANT_HG19:chr17:g.56283533T>A', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node5)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:7121' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_splice_site_variant_of' in plabels
    assert 'is_non_coding_variant_of' in plabels
    pids = [p.identifier for p in predicates]
    assert 'GAMMA:0000103' in pids
    assert 'SO:0001629' in pids

def test_batch_sequence_variant_to_gene(myvariant):
    variant_node = KNode('MYVARIANT_HG38:chr11:g.68032291C>G', type=node_types.SEQUENCE_VARIANT)
    variant_node2 = KNode('MYVARIANT_HG38:chrX:g.32389644G>A', type=node_types.SEQUENCE_VARIANT)
    variant_node3 = KNode('MYVARIANT_HG38:chr17:g.7674894G>A', type=node_types.SEQUENCE_VARIANT)

    batch_annotations = myvariant.batch_sequence_variant_to_gene([variant_node, variant_node2, variant_node3])
    relations = batch_annotations['MYVARIANT_HG38:chr11:g.68032291C>G']
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:7715' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_missense_variant_of' in plabels

    relations = batch_annotations['MYVARIANT_HG38:chrX:g.32389644G>A']
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:2928' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_nonsense_variant_of' in plabels

    relations = batch_annotations['MYVARIANT_HG38:chr17:g.7674894G>A']
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:11998' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_nonsense_variant_of' in plabels

def test_biolink(rosetta, biolink):
    variant_node = KNode('HGVS:NC_000023.9:g.32317682G>A', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    relations = biolink.sequence_variant_get_phenotype(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HP:0000750' in identifiers
    assert 'HP:0003236' in identifiers
    assert 'HP:0100748' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = set( [p.label for p in predicates] )
    assert 'has_phenotype' in plabels

def a_test_gwascatalog_variant_to_phenotype(gwascatalog, rosetta):
    # turned this off for now because it relies on gwascatalog being precached for CAIDs

    #relations = rosetta.cache.get('gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(CAID:CA248392703)')
    #identifiers = [node.id for r,node in relations]
    #assert 'EFO:0002690' in identifiers
    #predicates = [ relation.standard_predicate for relation,n in relations ] 
    #plabels = set( [p.label for p in predicates] )
    #assert 'has_phenotype' in plabels

    variant_node = KNode('CAID:CA248392703', type=node_types.SEQUENCE_VARIANT)
    relations = gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'EFO:0002690' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = set( [p.label for p in predicates] )
    assert 'has_phenotype' in plabels

    variant_node = KNode('CAID:CA16058750', type=node_types.SEQUENCE_VARIANT)
    relations = gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'EFO:0003898' in identifiers
    assert 'EFO:0001359' in identifiers
    assert 'ORPHANET:1572' in identifiers
    names = [node.name for r,node in relations]
    assert 'ankylosing spondylitis' in names
    assert 'chronic childhood arthritis' in names
    publications = [r.publications for r,node in relations]
    assert ['PMID:26301688'] in publications
    properties = [r.properties for r,node in relations]
    assert properties[0]['pvalue'] == 8.0E-11

    variant_node = KNode('DBSNP:rs369602258', type=node_types.SEQUENCE_VARIANT)
    results = gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(variant_node)
    assert len(results) == 0
  
def this_is_real_test_batch_gwascatalog_var_to_phenotype(rosetta, gwascatalog):

    gwascatalog.prepopulate_cache()
    
    relations = rosetta.cache.get('gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(CAID:CA16058750)')
    identifiers = [node.id for r,node in relations]
    assert 'EFO:0003898' in identifiers
    assert 'EFO:0001359' in identifiers
    assert 'ORPHANET:1572' in identifiers
    names = [node.name for r,node in relations]
    assert 'ankylosing spondylitis' in names
    assert 'chronic childhood arthritis' in names
    publications = [r.publications for r,node in relations]
    assert ['PMID:26301688'] in publications

    relations = rosetta.cache.get('gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(CAID:CA932131)')
    identifiers = [node.id for r,node in relations]
    assert 'EFO:0004340' in identifiers
    assert 'EFO:0003917' in identifiers
    assert 'EFO:0005939' in identifiers

def this_is_slow_test_gwascatalog_phenotype_to_variant(gwascatalog):
    #phenotype_node = KNode('EFO:0003898', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #relations = gwascatalog.disease_or_phenotypic_feature_to_sequence_variant(phenotype_node)
    #identifiers = [node.id for r,node in relations]
    #assert 'DBSNP:rs2066363' in identifiers
    #publications = [r.publications for r,node in relations]
    #assert ['PMID:26301688'] in publications

    phenotype_node = KNode('EFO:0004321', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    relations = gwascatalog.disease_or_phenotypic_feature_to_sequence_variant(phenotype_node)
    identifiers = [node.id for r,node in relations]
    assert 'DBSNP:rs2462021' in identifiers

def future_test_gene_to_sequence_variant(clingen):
   node = KNode('HGNC.SYMBOL:SRY', type=node_types.GENE)
   results = clingen.gene_to_sequence_variant(node)
   assert len(results) > 1000



