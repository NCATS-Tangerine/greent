from greent.graph_components import KNode
from greent import node_types
from greent.conftest import rosetta

def test_omnicorp(rosetta):
    pref = rosetta.cache.get('OmnicorpPrefixes')
    print(pref)
    assert len(pref) > 10

def test_omnicorp_2(rosetta):
    pref = rosetta.cache.get('OmnicorpSupport(CHEBI:2549,HGNC:6025)')
    print(pref)
    for p in pref:
        assert p.startswith('PMID')

def test_chebi(rosetta):
    syns = rosetta.cache.get("synonymize(CHEBI:15366)")
    print(len(syns))
    print(syns)

def test_cache(rosetta):
    syns = rosetta.cache.get("synonymize(HGNC:795)")
    print(len(syns))
    print(syns)

def test_kegg(rosetta):
    key='caster.upcast(kegg~enzyme_get_chemicals,chemical_substance)(HGNC:2843)'
    res = rosetta.cache.get(key)
    print(len(res))

def test_codeine(rosetta):
    key='caster.upcast(input_filter(kegg~chemical_get_chemical,metabolite),chemical_substance)(CHEBI:16714)'
    res = rosetta.cache.get(key)
    print(len(res))
    for r in res:
        print(r)

def test_pharos_key(rosetta):
    key='pharos.disease_get_gene(MONDO:0008903)'
    res = rosetta.cache.get(key)
    print(len(res))





def x_test_cache(rosetta):
    #syns = rosetta.cache.get("synonymize(UMLS:C0004096)")
    #print(syns)
    #s2 = rosetta.cache.get('synonymize(MedDRA:10003553)')
    #print (s2)
    #s3 = rosetta.cache.get('synonymize(MONDO:0004979)')
    #print (s3)
    s3 = rosetta.cache.get('synonymize(HGNC:6293)')
    print (s3)
    assert 0

def xtest_go_6293(rosetta):
    node = KNode("HGNC:6293",label="KCNN4", type=node_types.GENE)
    #s3 = rosetta.cache.get('synonymize(HGNC:10593)')
    rosetta.synonymizer.synonymize(node)
    biolink = rosetta.core.biolink
    r=biolink.gene_get_process_or_function(node)
    assert len(r) > 0

def xtest_go_1504(rosetta):
    node = KNode("HGNC:1504",label="---",type=node_types.GENE)
    s3 = rosetta.cache.get('synonymize(HGNC:10593)')
    rosetta.synonymizer.synonymize(node)
    biolink = rosetta.core.biolink
    r=biolink.gene_get_process_or_function(node)
    assert len(r) > 0

def xxtest_go(rosetta):
    node = KNode("HGNC:10593",label="SCN5A",type=node_types.GENE)
    s3 = rosetta.cache.get('synonymize(HGNC:10593)')
    rosetta.synonymizer.synonymize(node)
    print (node.get_synonyms_by_prefix('UNIPROTKB'))
    biolink = rosetta.core.biolink
    r=biolink.gene_get_process_or_function(node)
    assert len(r) > 0

def xtest_umls(rosetta):
    node = KNode("UMLS:C0015625",label="Fanconi Anemia", type=node_types.DISEASE)
    rosetta.synonymizer.synonymize(node)
    print(node.synonyms)
    assert node.id == 'MONDO:0019339'

def test_check_fanc_pheno(rosetta):
    key='biolink.disease_get_phenotype(MONDO:0019391)'
    s3 = rosetta.cache.get(key)
    node_ids = set()
    for edge,node in s3:
        node_ids.add(node.id)
    print(len(s3))
    print(len(node_ids))

def test_norcodeine_enzymes(rosetta):
    key='caster.input_filter(kegg~chemical_get_enzyme,metabolite)(CHEBI:80579)'
    s3 = rosetta.cache.get(key)
    print("?")
    print( len(s3) )
    for edge,node in s3:
        print(node.id, node.name)

def test_norcodeine_syn(rosetta):
    key='synonymize(CHEBI:80579)'
    s3 = rosetta.cache.get(key)
    print(len(s3))
    print(s3)
