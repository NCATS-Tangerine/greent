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
    variant_node = KNode('MYVARIANT_HG19:chr11:g.67799758C>G', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA6146346' in variant_node.get_synonyms_by_prefix('CAID') 
    assert 'DBSNP:rs369602258' in variant_node.get_synonyms_by_prefix('DBSNP')
    assert 'HGVS:NC_000011.10:g.68032291C>G' in variant_node.get_synonyms_by_prefix('HGVS')

    variant_node = KNode('MYVARIANT_HG38:chr11:g.68032291C>G', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA6146346' in variant_node.get_synonyms_by_prefix('CAID')
    assert 'DBSNP:rs369602258' in variant_node.get_synonyms_by_prefix('DBSNP')

    variant_node = KNode('DBSNP:rs369602258', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA6146346' in variant_node.get_synonyms_by_prefix('CAID')
    assert 'MYVARIANT_HG19:chr11:g.67799758C>G' in variant_node.get_synonyms_by_prefix('MYVARIANT_HG19')

    variant_node = KNode('HGVS:NC_000023.9:g.32317682G>A', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA267021' in variant_node.get_synonyms_by_prefix('CAID')
    assert 'MYVARIANT_HG19:chrX:g.32407761G>A' in variant_node.get_synonyms_by_prefix('MYVARIANT_HG19')
    assert 'CLINVARVARIANT:94623' in variant_node.get_synonyms_by_prefix('CLINVARVARIANT')

    variant_node = KNode('CLINVARVARIANT:18390', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CAID:CA128085' in variant_node.get_synonyms_by_prefix('CAID')

    variant_node = KNode('CAID:CA128085', type=node_types.SEQUENCE_VARIANT)
    rosetta.synonymizer.synonymize(variant_node)
    assert 'CLINVARVARIANT:18390' in variant_node.get_synonyms_by_prefix('CLINVARVARIANT')

def test_sequence_variant_to_gene(myvariant):
    variant_node = KNode('MYVARIANT_HG19:chr9:g.107620835G>A', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:29' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = set( [p.label for p in predicates] )
    assert 'is_missense_variant_of' in plabels

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

    variant_node = KNode('MYVARIANT_HG19:chr16:g.84205866A>G', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:30539' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'SO:0001583' in pids
    assert 'GAMMA:0000102' in pids

    variant_node = KNode('MYVARIANT_HG38:chr11:g.68032291C>G', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:7715' in identifiers
    assert 'HGNC:41796' in identifiers
    assert 'HGNC:49947' in identifiers
    assert 'HGNC:410' in identifiers

    variant_node = KNode('MYVARIANT_HG19:chr1:g.228192813C>T', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:15983' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_nearby_variant_of' in plabels
    assert 'intergenic_region' not in plabels
    pids = [p.identifier for p in predicates]
    assert 'GAMMA:0000102' in pids

    # this one has unmapped annotations that should be skipped
    variant_node = KNode('MYVARIANT_HG19:chr1:g.145440440C>T', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    pids = [p.identifier for p in predicates]
    assert 'GAMMA:0' not in pids

    variant_node = KNode('MYVARIANT_HG19:chr17:g.56283533T>A', type=node_types.SEQUENCE_VARIANT)
    relations = myvariant.sequence_variant_to_gene(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'HGNC:7121' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = [p.label for p in predicates]
    assert 'is_splice_site_variant_of' in plabels
    assert 'is_non_coding_variant_of' in plabels
    pids = [p.identifier for p in predicates]
    assert 'GAMMA:0000103' in pids
    assert 'SO:0001629' in pids

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

def test_gwascatalog_variant_to_phenotype(gwascatalog):
    variant_node = KNode('DBSNP:rs7329174', type=node_types.SEQUENCE_VARIANT)
    relations = gwascatalog.sequence_variant_to_phenotype(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'EFO:0002690' in identifiers
    predicates = [ relation.standard_predicate for relation,n in relations ] 
    plabels = set( [p.label for p in predicates] )
    assert 'has_phenotype' in plabels

    variant_node = KNode('DBSNP:rs2066363', type=node_types.SEQUENCE_VARIANT)
    relations = gwascatalog.sequence_variant_to_phenotype(variant_node)
    identifiers = [node.id for r,node in relations]
    assert 'EFO:0003898' in identifiers
    assert 'EFO:0001359' in identifiers
    assert 'ORPHANET:1572' in identifiers
    names = [node.name for r,node in relations]
    assert 'ankylosing spondylitis' in names
    assert 'chronic childhood arthritis' in names
    publications = [r.publications for r,node in relations]
    assert ['PMID:26301688'] in publications
    #properties = [r.properties for r,node in relations]
    #assert properties[0]['pvalue'] == 8.0E-11

    variant_node = KNode('DBSNP:rs369602258', type=node_types.SEQUENCE_VARIANT)
    results = gwascatalog.sequence_variant_to_phenotype(variant_node)
    assert len(results) == 0

def test_gwascatalog_phenotype_to_variant(gwascatalog):
    #phenotype_node = KNode('EFO:0003898', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #relations = gwascatalog.disease_or_phenotypic_feature_to_sequence_variant(phenotype_node)
    #identifiers = [node.id for r,node in relations]
    #assert 'DBSNP:rs2066363' in identifiers
    #publications = [r.publications for r,node in relations]
    #assert ['PMID:26301688'] in publications

    phenotype_node = KNode('EFO:0002690', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    relations = gwascatalog.disease_or_phenotypic_feature_to_sequence_variant(phenotype_node)
    identifiers = [node.id for r,node in relations]
    assert 'DBSNP:rs7329174' in identifiers

def future_test_gene_to_sequence_variant(clingen):
   node = KNode('HGNC.SYMBOL:SRY', type=node_types.GENE)
   results = clingen.gene_to_sequence_variant(node)
   assert len(results) > 1000



