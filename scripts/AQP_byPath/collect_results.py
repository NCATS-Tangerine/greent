from neo4j.v1 import GraphDatabase
import redis
import json
import os
import time

def get_hits(b_id,atype,edge_name,neo4j):
    cypher = f'MATCH (a:{atype})-[:{edge_name}]-(b {{id:"{b_id}"}}) RETURN distinct a.id'
    rlist = run_query(cypher,neo4j)
    return  [ r['a.id'] for r in rlist ]

def run_query(cypherquery,driver):
    start = time.time()
    with driver.session() as session:
        results = session.run(cypherquery)
    end = time.time()
    lr = list(results)
    print (f'  {end-start}, {len(lr)}')
    return lr

def get_redis(): 
    redis_host = '127.0.0.1' 
    redis_port = 6767 
    redis_db = 4 
    redis_driver = redis.StrictRedis(host=redis_host, port=int(redis_port), db=int(redis_db)) 
    return redis_driver 

def create_neo4j():
    url = 'bolt://127.0.0.1:7687'
    driver = GraphDatabase.driver(url, auth=("neo4j", os.environ['NEO4J_PASSWORD']))
    return driver

def get_topologies(b_id,max_graphs,atype,predicting_edge,red):
    key=f'MatchingTopologies({b_id},{max_graphs})'
    print(key)
    all_topologies = json.loads(red.get(key))
    return [tuple(x) for x in all_topologies]
    #return all_topologies

def assess_topology(topology,b_id,max_graphs,atype,predicting_edge,hits,redis):
    rkey = f'MatchResults({b_id},{max_graphs},{topology})'
    value = redis.get(rkey)
    if value is None:
        print(rkey)
        exit()
    all_results = json.loads(value)
    retres = []
    for one_result in all_results:
        a_s = set(one_result['results'])
        nhits = len( a_s.intersection(hits) )
        recall = nhits / len(hits)
        precision = nhits / len(a_s)
        retres.append( (one_result['nodes'],one_result['edges'],len(a_s),nhits,recall,precision) )
    return retres

def go(b_id, atype, predicting_edge, max_graphs):
    #Currently, I have not added atype or predicting edge to the redis keys, but I should
    red = get_redis()
    neo = create_neo4j()
    hits = get_hits(b_id,atype,predicting_edge,neo)
    #for the given b, graphsize, what topologies do I need to check on?
    topologies = get_topologies(b_id,max_graphs,atype,predicting_edge,red)
    with open(f'results_{b_id}_{max_graphs}','w') as rfile, open(f'defs_{b_id}_{max_graphs}','w') as gfile:
        rfile.write('query_id\tNumberResults\tNumberTruePostitives\tRecall\tPrecision\n')
        gfile.write('query_id\ttopology\tnodes\tedges\n')
        query_id = 0
        for topology in topologies:
            results = assess_topology(topology,b_id,max_graphs,atype,predicting_edge,hits,red)
            for res in results:
                #yuck yuck clean up
                gfile.write(f'{query_id}\t{topology}\t{res[0]}\t{res[1]}\n')
                rfile.write(f'{query_id}\t{res[2]}\t{res[3]}\t{res[4]}\t{res[5]}\n')
                query_id += 1

if __name__ == '__main__':
    go('MONDO:0005136','chemical_substance','treats',1000)
