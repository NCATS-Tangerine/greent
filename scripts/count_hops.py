import networkx as nx
from ast import literal_eval
from collections import defaultdict

all_patts = []
with open('topologies_pc.txt','r') as tf:
    nl = 0
    max_hops = 6
    for line in tf:
        nl += 1
        x = line.strip().split('\t')
        glist = literal_eval(x[1])
        g = nx.Graph()
        for e in glist:
            p = e.split('-')
            start = p[0]
            end = p[-1]
            g.add_edge(start,end)
        paths = nx.all_simple_paths(g,source='(a)',target='(b)')
        hops = [ len(p)-1 for p in paths ]
        count = defaultdict(int)
        for h in hops: count[h] += 1
        hopcount = tuple([ count[h] for h in range(max_hops,0,-1)])
        all_patts.append( (hopcount, x[0], glist) )

all_patts.sort()
with open('topologies_sorted.txt','w') as of:
    for hc,nn,g in all_patts:
        of.write(f'{hc}\t{nn}\t{g}\n')
