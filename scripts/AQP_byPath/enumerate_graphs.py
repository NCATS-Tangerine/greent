import redis
import json
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
    
def construct_for_pair(a_id,b_id,redis,nps,max_values,topologies,topo_counts):
    paths = get_paths(nps,a_id,b_id,redis)
    lp = len(paths)
    print(a_id)
    for p in nps:
        print(p,len(paths[p]))
    #print(f'{a_id},{lp} {comb(lp,2)} {comb(lp,3)}, {comb(lp,4)}')
    maxes = [0]+max_values #[1,5,2,2]
    current = [0,1,0,0,0]
    while True:
        num_graphs = 1
        for i in nps:
            num_graphs *= comb(len(paths[i]),current[i])
        if num_graphs > 0:
            print(current)
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
            #print(json.dumps(nx.readwrite.json_graph.node_link_data(g),indent=4))
            #print(json.dumps(nx.readwrite.json_graph.node_link_data(possible_graph),indent=4))
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
    print(pathgroups)
    for pathset in product(*pathgroups):
        #flatten the goofy representation
        #print('-----')
        #print(pathset)
        pathlist = [item for sublist in pathset for item in sublist]
        g=construct_from_pathset(pathlist)
        possibles = topologies[tuple(current)]
        #if there's only one possible, it better map
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
    paths = defaultdict(list)
    for np in nps:
        key = f'Paths({np},{a_id},{b_id})'
        value = redis.get(key)
        if value is not None:
            paths[np].extend(json.loads(value))
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

def construct_graphs(b_id,redis,atype):
    nps = [1,2,3,4]
    max_values = [1,5,2,2] # 1 0-hop, 5 1-hops, 2 2-hops, 2 3-hops
    topologies = read_topologies(max_values)
    a_ids = get_other_ends(nps,b_id,redis,atype)
    print('Number of other ends:',len(a_ids))
    maxp = 1
    topology_counts = defaultdict( lambda: defaultdict(set))
    for a_id in a_ids:
    #for a_id in ['CHEMBL:CHEMBL117452']:
        construct_for_pair(a_id,b_id,redis,nps,max_values,topologies,topology_counts)
    for tc in topology_counts:
        for (n,e) in topology_counts[tc]:
            print('-----------')
            print(tc)
            print('  ',n)
            print('  ',e)
            print('    ',topology_counts[tc][(n,e)])


def get_other_ends(nps,b_id,redis,atype):
    a_ids = set()
    for np in nps:
        results = redis.get(f"EndPoints({np},{b_id},{atype})")
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

def get_redis():
    redis_host = '127.0.0.1'
    redis_port = 6767
    redis_db = 4
    redis_driver = redis.StrictRedis(host=redis_host, port=int(redis_port), db=int(redis_db))
    return redis_driver

if __name__ == '__main__':
    x = 'MONDO:0005136'
    red = get_redis()
    construct_graphs(x,red,'chemical_substance')
