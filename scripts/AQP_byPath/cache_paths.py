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
    cypher += ' RETURN DISTINCT a.id as aid, type(e0) as te0 '
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
    defs = [{}]
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
    return a_id, defs

def store_results(redis,results,n,b_id,atype):
    path_definitions = defaultdict(list)
    print('  Convert Results')
    for result in results:
        a_id,defs = convert_neo_result_to_dicts(result)
        path_definitions[a_id].extend(defs)
    aids = set()
    print('  Write Results')
    for a_id in path_definitions:
        aids.add(a_id)
        rep = json.dumps(path_definitions[a_id])
        key = f'Paths({n},{a_id},{b_id})'
        redis.set(key,rep)
    bkey = f'EndPoints({n},{b_id},{atype})'
    redis.set(bkey,json.dumps(list(aids)))

def single_endpoint(atype,btype,b_id,censored_edges,nhops,neo4j,redis,max_degree=5000):
    """Find paths from a_types to b_types where b is bound to b_id, ignoring censored edges.
        Paths come from neo4j and are put into redis"""
    print(b_id, nhops)
    print(' Make Query')
    query = construct_cypher(atype,btype,b_id,censored_edges,nhops,max_degree)
    print(query)
    print(' Run Query')
    results = run_query(query,neo4j)
    print(' Store Results')
    store_results(redis,results,nhops,b_id,atype)

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

if __name__ == '__main__':
    n4j = create_neo4j()
    red = create_redis()
    for gnh in [1,2,3,4]:
        single_endpoint('chemical_substance','disease','MONDO:0005136',['contributes_to','treats'],gnh,n4j,red,max_degree=300)
