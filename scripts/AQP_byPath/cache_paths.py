from neo4j.v1 import GraphDatabase
import redis
import os
import time
import json
import copy
from collections import defaultdict

def construct_cypher(atype,btype,b_id,censored_edges,nhops,max_degree):
    cypher = f'MATCH (a:{atype})-[e0]-'
    for hop in range(nhops-1):
        cypher += f'(n{hop})-[e{hop+1}]-'
    cypher += f'(b:{btype} {{id:"{b_id}"}}) '
    where = 'WHERE'
    for hop in range(nhops - 1):
        cypher += f' {where} size( (n{hop})-[]-() ) < {max_degree} '
        if where == 'WHERE':
            where = 'AND'
    for hop in range(nhops):
        for ce in censored_edges:
            cypher += f' {where} type(e{hop}) <> "{ce}"'
            if where == 'WHERE':
                where = 'AND'
    cypher += ' RETURN DISTINCT a.id as aid, a.name as aname, type(e0) as te0 '
    for nhop in range(nhops-1):
        cypher += f', labels(n{nhop}) as ln{nhop}, n{nhop}.id as n{nhop}id, type(e{nhop+1}) as te{nhop+1}'
    return cypher

def run_query(cypherquery,driver):
    start = time.time()
    with driver.session() as session:
        results = session.run(cypherquery)
    end = time.time()
    lr = list(results)
    print (f'  {end-start}, {len(lr)}')
    return lr

def convert_neo_result_to_dicts(result):
    rdict = {i[0]:i[1] for i in result.items()}
    a_id = rdict['aid']
    del rdict['aid']
    a_name = rdict['aname']
    del rdict['aname']
    #print(a_id,a_name)
    defs = [{}]
    #All of this stuff with def being a list is to handle the case when a node has multiple labels, so then
    # it generates multiple definitions
    for k in rdict:
        if k.startswith('l') :
            f = rdict[k]
            f.remove('named_thing')
            for d in defs:
                d[k] = f[0]
            all_newdefs = []
            for label in f[1:]:
                newdefs =copy.deepcopy(defs)
                for d in newdefs:
                    d[k] = label
                all_newdefs.append(newdefs)
            for nd in all_newdefs:
                defs += nd
        else:
            for d in defs:
                d[k] = rdict[k]
    # At this point, defs is a list (usually 1 element, but whatev)
    # it has in it something like this: {'te0': 'decreases_activity_of','ln0':'gene', 'n0id':'HGNC:11892'...}
    # I want a more general representation that (1) removes the ids (2) can be used as the key into a map to group
    # similar paths (similar at the type level)
    templates2paths = defaultdict(list)
    for path in defs:
        n_nodes = int((len(path)-1)/3)
        nodestring = ','.join([ path[f'ln{i}'] for i in range(n_nodes)])
        edgestring = ','.join([ path[f'te{i}'] for i in range(n_nodes+1)])
        key = nodestring+'\t'+edgestring
        templates2paths[ key ].append(path)
    return a_id, templates2paths

def store_results(redis,results,n,b_id,atype,cnodes):
    path_definitions = defaultdict(lambda: defaultdict(list))
    print('  Convert Results')
    for result in results:
        a_id,defs = convert_neo_result_to_dicts(result)
        for definition,newpaths in defs.items():
            path_definitions[a_id][definition].extend(newpaths)
    aids = set()
    print('  Write Results')
    for a_id in path_definitions:
        if a_id in cnodes:
            continue
        aids.add(a_id)
        rep = json.dumps(path_definitions[a_id])
        key = f'Paths({n},{a_id},{b_id})'
        redis.set(key,rep)
    bkey = f'EndPoints({n},{b_id},{atype})'
    redis.set(bkey,json.dumps(list(aids)))

def single_endpoint(atype,btype,b_id,censored_nodes,censored_edges,predicting_edge,nhops,neo4j,redis,max_degree=5000):
    """Find paths from a_types to b_types where b is bound to b_id, ignoring censored edges.
        Paths come from neo4j and are put into redis"""
    print(b_id, nhops)
    ces = censored_edges.copy()
    if nhops == 1:
        ces.append(predicting_edge)
    print(' Make Query')
    query = construct_cypher(atype,btype,b_id,ces,nhops,max_degree)
    print(query)
    print(' Run Query')
    results = run_query(query,neo4j)
    print(' Store Results')
    store_results(redis,results,nhops,b_id,atype,censored_nodes)

def go():
    #Query Statement
    # Find all 1 hop paths from any chemical substance to any disease
    atype = 'chemical_substance'
    btype = 'disease'
    censored_edges = ['contributes_to', 'treats']
        
    nhops = 1

def create_neo4j():
    url = 'bolt://127.0.0.1:7687'
    driver = GraphDatabase.driver(url, auth=("neo4j", os.environ['NEO4J_PASSWORD']))
    return driver

def create_redis():
    redis_host = '127.0.0.1'
    redis_port = 6767
    redis_db = 4
    redis_driver = redis.StrictRedis(host=redis_host, port=int(redis_port), db=int(redis_db))
    return redis_driver

censored_as = [
'CHEBI:23367',
'CHEBI:24532',
'CHEBI:25367',
'CHEBI:25806',
'CHEBI:33285',
'CHEBI:33304',
'CHEBI:33579',
'CHEBI:33582',
'CHEBI:33595',
'CHEBI:33608',
'CHEBI:33635',
'CHEBI:33671',
'CHEBI:33674',
'CHEBI:33675',
'CHEBI:33822',
'CHEBI:33832',
'CHEBI:36357',
'CHEBI:36587',
'CHEBI:36834',
'CHEBI:36962',
'CHEBI:36963',
'CHEBI:37577',
'CHEBI:38104',
'CHEBI:38166',
'CHEBI:50860',
'CHEBI:51958',
'CHEBI:72695',
'GOCHE:22586',
'GOCHE:23354',
'GOCHE:23888',
'GOCHE:24020',
'GOCHE:24432',
'GOCHE:25212',
'GOCHE:27027',
'GOCHE:27314',
'GOCHE:33229',
'GOCHE:33232',
'GOCHE:33284',
'GOCHE:50267',
'GOCHE:51086',
'GOCHE:52206',
'GOCHE:52211',
'GOCHE:52217',
'GOCHE:63726',
'GOCHE:75763',
'GOCHE:75767',
'GOCHE:75768',
'GOCHE:75771',
'GOCHE:77746',
'GOCHE:78295',
'GOCHE:78675',
'GOCHE:83038',
'GOCHE:83039',
'GOCHE:83057',
'GOCHE:84735' ]

if __name__ == '__main__':
    n4j = create_neo4j()
    red = create_redis()
    malaria = 'MONDO:0005136'
    asthma = 'MONDO:0004979'
    v = [(1,3000),(2,3000),(3,3000),(4,300)]
    #v = [(4,200)]
    for gnh,c in v:
        single_endpoint('chemical_substance','disease',malaria,censored_as,['contributes_to'],'treats',gnh,n4j,red,max_degree=c)
