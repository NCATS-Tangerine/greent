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

def get_chemicals(url):
    """This is all the variants.  We might want to filter on source"""
    cquery = f'''match (a:chemical_substance) where a.smiles is not NULL RETURN a.id, a.name,a.smiles'''
    records = run_query(url,cquery)
    with open('smiles.txt','w') as outf:
        for r in records:
            print(r)
            outf.write(f'{r["a.id"]}\t{r["a.name"]}\t{r["a.smiles"]}\n')

if __name__ == '__main__':
    url = 'bolt://robokopdb.renci.org:7687'
    get_chemicals(url)