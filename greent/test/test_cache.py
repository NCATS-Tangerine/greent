from greent.graph_components import KNode
from greent import node_types
from greent.conftest import rosetta

def xtest_cache(rosetta):
    #syns = rosetta.cache.get("synonymize(UMLS:C0004096)")
    #print(syns)
    #s2 = rosetta.cache.get('synonymize(MedDRA:10003553)')
    #print (s2)
    #s3 = rosetta.cache.get('synonymize(MONDO:0004979)')
    #print (s3)
    s3 = rosetta.cache.get('synonymize(HGNC:10593)')
    print (s3)
    assert 0

def test_go_1504(rosetta):
    node = KNode("HGNC:1504",label="---",type=node_types.GENE)
    s3 = rosetta.cache.get('synonymize(HGNC:10593)')
    rosetta.synonymizer.synonymize(node)
    biolink = rosetta.core.biolink
    r=biolink.gene_get_process_or_function(node)
    assert len(r) > 0

def xtest_go(rosetta):
    node = KNode("HGNC:10593",label="SCN5A",type=node_types.GENE)
    s3 = rosetta.cache.get('synonymize(HGNC:10593)')
    rosetta.synonymizer.synonymize(node)
    print (node.get_synonyms_by_prefix('UNIPROTKB'))
    biolink = rosetta.core.biolink
    r=biolink.gene_get_process_or_function(node)
    assert len(r) > 0
