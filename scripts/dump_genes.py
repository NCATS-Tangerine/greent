from greent.rosetta import Rosetta
import json
from collections import defaultdict

def setup():
    rosetta = Rosetta()
    neodriver = rosetta.type_graph.driver;
    return neodriver

def dumpem(dtype = 'gene'):
    driver = setup()
    cypher = f'MATCH (a:{dtype})-[r]-(b) return a,r,b'
    with driver.session() as session:
        result = session.run(cypher)
    records = list(result)
    genes = defaultdict(list)
    for record in records:
        gid = record['a']['id']
        edgetype= record['r'].type
        other = record['b']['id']
        genes[gid].append( {'predicate': edgetype, 'node': other})
    with open('genedump.json','w') as outf:
        json.dump(genes,outf,indent=4)


if __name__ == '__main__':
    dumpem()