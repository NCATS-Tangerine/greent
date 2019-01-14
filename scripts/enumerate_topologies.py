import networkx as nx
import networkx.algorithms.isomorphism as iso

def create_graph(i,endnames,extranames):
    g = nx.Graph()
    for node in endnames:
        g.add_node(node, nodetype=node)
    g.add_nodes_from(extranames, nodetype='middle')
    edge_num = 0
    nodenames = endnames + extranames
    for ni,node_i in enumerate(nodenames):
        for nj,node_j in enumerate(nodenames[ni+1:]):
            if i & (1<<edge_num): #if the bit corresponding to this edge is set...
                #print(node_i,node_j,' are connected')
                g.add_edge(node_i,node_j)
            edge_num += 1
    return g

def degree_check(g):
    for node in g.nodes():
        if node == 'a' or node=='b':
            continue
        if g.degree(node) < 2:
            return False
    return True

def has_isomorphism_with(g,graphs):
    for oldgraph in graphs:
        if nx.is_isomorphic(g,oldgraph,node_match=iso.categorical_node_match('nodetype','dumb')):
            return True
    return False

def generate_topologies(nextra,outf):
    """Find all the non-isomorphic topologies that can be turned into cypher queries for patterns.
    :param: n: The number of extra (non-a,b) nodes.  n>=0."""
    endnames=['a','b']
    extranames = []
    for i in range(nextra):
        extranames.append( f'n{i}')
    #If there are n nodes, then there are n*(n-1)/2 possible edge slots.
    n = len(extranames) + len(endnames)
    num_edge_slots = n * (n-1) / 2
    num_combinations = 2 ** num_edge_slots
    graphs = []
    for i in range(int(num_combinations)):
        g = create_graph(i,endnames,extranames)
        if not nx.is_connected(g):
            continue
        if not degree_check(g):
            continue
        if has_isomorphism_with(g,graphs):
            continue
        graphs.append(g)
    print(nextra, int(num_combinations), len(graphs))
    for graph in graphs:
        pattern = []
        e_num = 0
        for edge in graph.edges():
            epatt = f"({edge[0]})-[e{e_num}]-({edge[1]})"
            pattern.append(epatt)
            e_num += 1
        outf.write(f'{nextra}\t{pattern}\n')
        outf.flush()


def bin(s):
    return str(s) if s<=1 else bin(s>>1) + str(s&1)

if __name__ == '__main__':
    with open('topologies.txt','w') as topfile:
        generate_topologies(0,topfile)
        generate_topologies(1,topfile)
        generate_topologies(2,topfile)
        generate_topologies(3,topfile)
        generate_topologies(4,topfile)
        generate_topologies(5,topfile)

