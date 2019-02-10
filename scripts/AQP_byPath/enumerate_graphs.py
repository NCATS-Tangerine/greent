from neo4j.v1 import GraphDatabase
import redis
import json
import time
import os
from scipy.misc import comb
from collections import defaultdict
from itertools import combinations, product
import networkx as nx
from networkx.algorithms import isomorphism
from ast import literal_eval

def increment_current(current,maxes,place):
    if place >= len(current):
        return None
    current[place] += 1
    if current[place] > maxes[place]:
        current[place] = 0
        return increment_current(current,maxes,place+1)
    else:
        return current

def apply_filters(paths,filters):
    newpaths = {}
    for np in paths:
        pnp = {}
        allkeys = paths[np].keys()
        for goodkey in filters[np]:
            if goodkey in paths[np]:
                pnp[goodkey] = paths[np][goodkey]
        newpaths[np] = pnp
    return newpaths

def aggregate(p,nps):
    newp = {}
    for n in nps:
        newp[n] = []
        if n not in p:
            continue
        for v in p[n].values():
            newp[n].extend( v )
    return newp

def construct_for_pair(a_id,b_id,redis,nps,max_values,topologies,topo_counts,filters,max_graphs):
    print('--------')
    print(a_id)
    paths = get_paths(nps,a_id,b_id,redis)
    paths = apply_filters(paths,filters)
    paths = aggregate(paths,nps)
    lp = len(paths)
    #for p in nps:
    #    print(p,len(paths[p]))
    #print(f'{a_id},{lp} {comb(lp,2)} {comb(lp,3)}, {comb(lp,4)}')
    maxes = [0]+max_values #[1,5,2,2]
    current = [0,1,0,0,0]
    while True:
        num_graphs = 1
        for i in nps:
            num_graphs *= comb(len(paths[i]),current[i])
        if num_graphs > 0:
            print(current, '   ',num_graphs)
            if num_graphs < max_graphs:
                construct_current(current,paths,topologies,topo_counts,a_id)
        current = increment_current(current,maxes,1)
        if current is None:
            break

def get_isomorphism(g,possibles):
    """g: a networkx graph that we are trying to match
       possibles: a list of (networkx graph, name key) of different topologies that this might match"""
    #print('CHECKINGCHECKINGCHECKING')
    #print('Possibles: ',len(possibles))
    for possible_graph, possible_name in possibles:
        matcher=isomorphism.GraphMatcher(possible_graph,g,node_match=isomorphism.categorical_node_match('matchtype','dumb'))
        if matcher.is_isomorphic():
            return possible_graph, possible_name, matcher.mapping
    #It's possible not to find one. suppose we're looking for 2 1-hops.
    #  But we have the same node, with different edges.  That collapses to a 1-hop with multiple edges, but that's not
    #  in our possible list at that point.  Ignore for now
    return None,None,None

def construct_current(current,paths,topologies,topo_counts,a_id):
    pathgroups = []
    for nhops_minus_1,cur_i in enumerate(current[1:]):
        nhops = nhops_minus_1 + 1
        if len(paths[nhops]):
            pathgroups.append( combinations(paths[nhops],cur_i) )
    #print(pathgroups)
    for pathset in product(*pathgroups):
        #flatten the goofy representation
        pathlist = [item for sublist in pathset for item in sublist]
        g=construct_from_pathset(pathlist)
        possibles = topologies[tuple(current)]
        matching_graph, matching_name, mapping = get_isomorphism(g,possibles)
        if matching_graph is None:
            continue
        nodetypes = []
        nn = 0
        while f'n{nn}' in mapping:
            try:
                nodetypes.append( g.nodes[mapping[f'n{nn}']]['node_type'] )
            except:
                print(json.dumps(nx.readwrite.json_graph.node_link_data(g),indent=4))
                print(json.dumps(nx.readwrite.json_graph.node_link_data(matching_graph),indent=4))
                print(mapping)
                exit()
            nn += 1
        edgetypes = []
        emap = {}
        for e in matching_graph.edges(data=True):
            ename = e[2]['name']
            m = (mapping[e[0]],mapping[e[1]])
            etype = g.edges[m]['edge_type']
            emap[ename]=etype
        for i in range(len(emap)):
            edgetypes.append(emap[f'e{i}'])
        topo_counts[tuple(matching_name)][(tuple(nodetypes),tuple(edgetypes))].add(a_id)

def construct_from_pathset(pset):
    """The input is an iterable of paths that we want to merge.
        The output is a merged networkx graph"""
    g = nx.Graph()
    g.add_node('a',matchtype='a')
    g.add_node('b',matchtype='b')
    for path in pset:
        nodes=['a']
        #add the nodes. Because we need to join across paths by id, we want 
        # the id (HGNC:123) to be the id here as well.
        lp = len(path)
        nnodes=int((lp-1)/3)
        for i in range(nnodes):
            node = path[f'n{i}id']
            g.add_node( node, node_type=path[f'ln{i}'],matchtype='n' )
            nodes.append(node)
        nodes.append('b')
        #Add the edges
        for i in range(nnodes+1):
            g.add_edge(nodes[i],nodes[i+1],edge_type=path[f'te{i}'])
    return g


def get_paths(nps,a_id,b_id,redis):
    paths = defaultdict(lambda: defaultdict(list))
    for np in nps:
        key = f'Paths({np},{a_id},{b_id})'
        value = redis.get(key)
        if value is not None:
            x = json.loads(value)
            #x should be a dictionary
            for k in x:
                paths[np][k].extend(x[k])
    return paths

def rep_to_graph(edgelist):
    g = nx.Graph()
    for el in edgelist:
        x = el.split('-')
        g.add_edge(x[0][1:-1],x[2][1:-1],name=x[1][1:-1])
    mts = {}
    for node in g.nodes():
        if node == 'a':
            mts['a'] = 'a'
        elif node == 'b':
            mts['b'] = 'b'
        else:
            mts[node] = 'n'
    nx.set_node_attributes(g,mts,name='matchtype')
    return g

def read_topologies(max_values):
    tops = defaultdict(list)
    mv = max_values+[0,0]
    with open('scripts/topologies_sorted.txt','r') as inf:
        for line in inf:
            #The path key is (1) a list (2) too long and (3) reversed (4) no placeholder in front
            x = line.strip().split('\t')
            counts = list(literal_eval(x[0]))
            counts.reverse()
            ok = True
            for i,j in zip(counts,mv):
                if i>j:
                    ok = False
            if not ok:
                continue
            key=tuple([0]+counts[:len(max_values)])
            graph_string = literal_eval(x[2])
            graph = rep_to_graph(graph_string)
            tops[key].append( (graph, graph_string))
    return tops

def get_hits(b_id,atype,edge_name,neo4j):
    cypher = f'MATCH (a:{atype})-[:{edge_name}]-(b {{id:"{b_id}"}}) RETURN distinct a.id'
    rlist = run_query(cypher,neo4j)
    return  [ r['a.id'] for r in rlist ]

def construct_hit_filters(b_id,redis,neo4j,atype,edge_name):
    a_ids = get_hits(b_id,atype,edge_name,neo4j)
    hit_filters = defaultdict(set)
    for a_id in a_ids:
        paths = get_paths([1,2,3,4],a_id,b_id,redis)
        for i in paths:
            #print(paths[i])
            hit_filters[i].update( paths[i].keys() )
    return hit_filters

def put_topologies_in_redis(redis,topologies):
    #Don't really need to do this all the time.  Should have enumerate_topologies do this directly I think. Then pull from there for this and aggregation
    for pcount in topologies:
        for graph,gstring in topologies[pcount]:
            rkey = f'PathCount({gstring})'
            redis.set(rkey,json.dumps(pcount))

def construct_graphs(b_id,a_ids,redis,filters,nps,max_graphs):
    max_values = [1,5,2,2] # 1 0-hop, 5 1-hops, 2 2-hops, 2 3-hops
    topologies = read_topologies(max_values)
    put_topologies_in_redis(redis,topologies)
    print('Number of other ends:',len(a_ids))
    maxp = 1
    topology_counts = defaultdict( lambda: defaultdict(set))
    #skips = set()
    for a_id in a_ids:
        construct_for_pair(a_id,b_id,redis,nps,max_values,topologies,topology_counts,filters,max_graphs)
    key=f'MatchingTopologies({b_id},{max_graphs})'
    #Each topology_counts key is a tuple of edges. Make each key a list, then make a list of those
    # lists (i.e. a list of all topologies) that can be serialized as json for the value
    topos = [ list(k) for k in topology_counts.keys() ]
    redis.set(key,json.dumps(topos))
    for tc in topology_counts:
        mkey = f'MatchResults({b_id},{max_graphs},{tc})'
        output = [{'nodes':n,'edges':e,'results':list(topology_counts[tc][(n,e)]) } for n,e in topology_counts[tc]]
        redis.set(mkey,json.dumps(output))
        

def get_other_ends(nps,b_id,red,atype):
    a_ids = set()
    for np in nps:
        results = red.get(f"EndPoints({np},{b_id},{atype})")
        n_a_ids = json.loads(results)
        a_ids.update(n_a_ids)
    return a_ids

def count_paths(b_id,np,n_a_ids,redis,):
    total_paths =0
    for a_id in n_a_ids:
        key = f'Paths({np},{a_id},{b_id})'
        paths = json.loads(redis.get(key))
        total_paths += len(paths)
    print(np,total_paths)

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

def go(disease):
    red = get_redis()
    neo = create_neo4j()
    a_type = 'chemical_substance'
    p_edge = 'treats'
    filters = construct_hit_filters(disease,red,neo,a_type,p_edge)
    nps = [1,2,3,4]
    a_ids = get_other_ends(nps,disease,red,a_type)
    max_graphs = 100000
#    a_ids = set( list(a_ids)[:10] )
    construct_graphs(disease,a_ids,red,filters,nps,max_graphs)

if __name__ == '__main__':
    go('MONDO:0005136')

