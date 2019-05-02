from neo4j.v1 import GraphDatabase
import os

# Maybe make this a notebook?

def get_driver(url):
    driver = GraphDatabase.driver(url, auth=("neo4j", os.environ['NEO4J_PASSWORD']))
    return driver

def run_query(url,cypherquery):
    driver = get_driver(url)
    with driver.session() as session:
        results = session.run(cypherquery)
    return list(results)

def get_variants_for_phenotype(url,identifier):
    """This is all the variants.  We might want to filter on source"""
    cquery = f'''match p=(a:disease_or_phenotypic_feature {{id:"{identifier}" }})--(v:sequence_variant) RETURN p'''
    records = run_query(url,cquery)
    nodelists = [list(record['p'].nodes) for record in records]
    allnodes = sum(nodelists, [])
    variants = list(filter(lambda n: 'sequence_variant' in n.labels, allnodes))
    return variants

#def get_variants_for_phenotype(url,identifier):
#    cquery = f'''match p=(a:disease_or_phenotypic_feature {{id:"{identifier}" }})--(v:sequence_variant) RETURN p'''
#    records = run_query(url,cquery)
#    for record in records:
#        path = record['p']
#        print(path.nodes)
#        print(path.relationships)
#        break

def testq(url):
    q = 'match (s:sequence_variant)-[e]-(a:disease {id:"MONDO:0011122"})-[ed]-(c:chemical_substance)-[f]-(s2:sequence_variant)-[ld]-(s) where e.namespace = "obesity_diet" and e.p_value < 1e-4 and f.p_value < 1e-4 return *'
    result = run_query(url,q)
    print(len(result))

def go():
    url = 'bolt://obesityhub.edc.renci.org:7687'
    testq(url)
    #variants = get_variants_for_phenotype(url,'EFO:0003940') #Physical Activity
    #for variant in variants:
    #    if variant.properties['id'].startswith('HGVS'):
    #        print(variant.properties['id'])
    #print(len(variants))

if __name__ == '__main__':
    go()
