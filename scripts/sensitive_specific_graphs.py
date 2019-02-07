from neo4j.v1 import GraphDatabase
import os
import json
import pickle
import time
import sys
from itertools import product
from ast import literal_eval

def extract_nodes_and_edge_from_hop(hop):
    """Assumes (n*)-[e*]-(n*)"""
    names = [n[1:-1] for n in hop.split('-')]
    nodes = [names[0],names[-1]]
    edge = names[1]
    return nodes,edge


class Pattern():
    def __init__(self,cypher):
        """cypher is an array of one-hop cypher statements using n* as node names and e* as edge names.
        The start and end nodes (corresponding to the nodes in the training edge) are a and b."""
        self.pattern = cypher
        self.get_edges()
        self.get_nodes()
    def get_edges(self):
        self.edges = []
        edge_num = 0
        all_pattern = ','.join(self.pattern)
        while True:
            edge_name = f'e{edge_num}'
            if f'[{edge_name}]' in all_pattern:
                self.edges.append(edge_name)
                edge_num += 1
            else:
                return
    def get_nodes(self):
        self.nodes = []
        node_num = 0
        all_pattern = ','.join(self.pattern)
        while True:
            node_name = f'n{node_num}'
            if f'({node_name})' in all_pattern:
                self.nodes.append(node_name)
                node_num += 1
            else:
                return
    def get_possible_types(self,ntypes,etypes,atype,btype):
        type_results = []
        for na in product(ntypes,repeat = len(self.nodes)):
            assignment = { f'n{i}': na[i] for i in range(len(self.nodes))}
            assignment['a'] = atype
            assignment['b'] = btype
            #Map from edge to possible edge types for this assignment of node types
            edge_possibilities = {}
            for hop in self.pattern:
                nodes,edge = extract_nodes_and_edge_from_hop(hop)
                #the node types for this edge for this assignment of types
                node_types= [assignment[node] for node in nodes]
                node_types.sort()
                edge_possibilities[edge] = etypes[ tuple(node_types)]
            #A flattened version of edge_possibilities in the order of self.edges
            edge_prep = [ edge_possibilities[e] for e in self.edges]
            #ea will be a tuple choosing with a value for each edge type
            for ea in product(*edge_prep):
                full_assignment = assignment.copy()
                for edgenum, edgeident in enumerate(self.edges):
                    full_assignment[edgeident] = ea[edgenum]
                type_results.append(full_assignment)
        return type_results
    def get_type_cypher(self,target_link):
        cypher = f"MATCH p={target_link}"
        for ip,patt in enumerate(self.pattern):
            cypher += f", q{ip}={patt}"
        cypher += " RETURN "
        for edge_name in self.edges:
            cypher += f"type({edge_name}) as {edge_name}, "
        for node_name in self.nodes:
            cypher += f"labels({node_name}) as {node_name}, "
        cypher += "count(p) as cp"
        return cypher
    def get_filled_pattern(self,res):
        fpatternl = self.pattern.copy()
        for edge in self.edges:
            oe = f'[{edge}]'
            et = res[f'{edge}']
            ne = f'[{edge}:{et}]'
            for i,fpattern in enumerate(fpatternl):
                fpatternl[i] = fpattern.replace(oe, ne)
        for node in self.nodes:
            on = f'({node})'
            nlabel = res[node]
            nn = f'({node}:{nlabel})'
            for i,fpattern in enumerate(fpatternl):
                fpatternl[i] = fpattern.replace(on, nn)
        return fpatternl


def get_driver(url):
    driver = GraphDatabase.driver(url, auth=("neo4j", os.environ['NEO4J_PASSWORD']))
    return driver

def run_query(url,cypherquery,outf=sys.stdout):
    driver = get_driver(url)
    start = time.time()
    with driver.session() as session:
        results = session.run(cypherquery)
    end = time.time()
    outf.write(f'{cypherquery}\t{end-start}\n')
    return list(results)

def get_sensitivity_specificity(res,target_link,pattern,url,queryf,atype,btype):
    #fpattern = f"(a:chemical_substance)-[x0:{tx0}]-(n0:{type0})-[x1:{tx1}]-(b:disease)"
    fpattern = pattern.get_filled_pattern(res)
    #How many of the known target links (nlinks) does this pattern recover?
    cypher = f"MATCH p={target_link}"
    for ip,fp in enumerate(fpattern):
        cypher += f', q{ip}={fp}'
    cypher += " WITH nodes(p) AS n RETURN COUNT(DISTINCT n) AS c"
    link_count = run_query(url,cypher,queryf)
    known_returned = link_count[0]['c']
    #How many node pairs overall are matched by this pattern?
    total_cypher = f"MATCH "
    for ip,fp in enumerate(fpattern):
        fp = fp.replace('(a)',f'(a:{atype})')
        fp = fp.replace('(b)',f'(b:{btype})')
        if ip != 0:
            total_cypher += ", "
        total_cypher += f"q{ip}={fp}"
    total_cypher += f" with [a,b] as n return count(distinct n) as c"
    total_count = run_query(url,total_cypher,queryf)
    total_returned = total_count[0]['c']
    return known_returned,total_returned

def run_pattern(target_link,pattern,url,countf,patternf,queryf,ntypes,etypes,atype,btype):
    # How many links are there?
    linkcypher = f"MATCH p = {target_link} with nodes(p) as n return count(distinct n) as c"
    link_count = run_query(url,linkcypher,queryf)
    num_links = link_count[0]['c']
    print(num_links)
    #First, find the edge and node types
    #cypher = pattern.get_type_cypher(target_link)
    #runres = run_query(url,cypher)
    runres = pattern.get_possible_types(ntypes,etypes,atype,btype)
    print(len(runres))
    for ires,res in enumerate(runres):
        known_count, total_returned = get_sensitivity_specificity(res,target_link,pattern,url,queryf,atype,btype)
        recall = known_count / num_links
        if total_returned != 0:
            precision = known_count / total_returned
        else:
            precision = 0.
        pstring = ','.join(pattern.pattern)
        countf.write(f"{pstring}\t{ires}\t{known_count}\t{num_links}\t{total_returned}\t{recall}\t{precision}\n")
        pres = { k:res[k] for k in res }
        patternf.write(f"{pstring}\t{ires}\t{json.dumps(pres)}\n")
        patternf.flush()
        countf.flush()
        queryf.flush()

def get_node_types(url):
    node_type_set = set()
    nodequery = 'MATCH (n) RETURN DISTINCT LABELS(n) AS l'
    node_results = run_query(url,nodequery)
    for node_result in node_results:
        nr = node_result['l']
        if 'named_thing' in nr:
            node_type_set.update(nr)
    node_types = list(node_type_set)
    node_types.remove('named_thing')
    node_types.remove('Concept')
    node_types.sort()
    return node_types

def get_edge_types(url,node_types):
    edge_types = {}
    for i,nt0 in enumerate(node_types):
        for nt1 in node_types[i:]:
            k = (nt0,nt1)
            equery = f'MATCH (a:{nt0})-[x]-(b:{nt1}) RETURN DISTINCT TYPE(x) as etype'
            edge_results = run_query(url,equery)
            et = [r['etype'] for r in edge_results]
            if 'is_a' in et:
                et.remove('is_a')
            print(et)
            edge_types[k] = et
    return edge_types

def get_node_and_edge_types(url):
    node_types = get_node_types(url)
    edge_types = get_edge_types(url,node_types)
    print(edge_types)
    return node_types, edge_types

def go(rebuild = True):
    url = 'bolt://robokop.renci.org:7687'
    #Use pickle instead of json here because the edge keys are tuples, which json doesn't want to serialize.
    if rebuild:
        node_types,edge_types = get_node_and_edge_types(url)
        with open('edgetypes.pickle','wb') as edgefile, open('nodelabels.pickle','wb') as nodefile:
            pickle.dump(node_types, nodefile)
            pickle.dump(edge_types, edgefile)
    else:
        with open('edgetypes.pickle','rb') as edgefile, open('nodelabels.pickle','rb') as nodefile:
            node_types=pickle.load(nodefile)
            edge_types=pickle.load( edgefile)
    atype = 'chemical_substance'
    btype = 'disease'
    target_link = f"(a:{atype})-[:treats]->(b:{btype})"
    with open('n_hopa.txt','w') as countf, \
         open('n_hopa_defs.txt','w') as patternf, \
         open('queries.txt','w') as queryf, \
         open('topologies.txt','r') as topof:
        countf.write('Topology\tPID\tTPCount\tTrueLinkCount\tAllReturnedCount\tRecall\tPrecision\n')
        queryf.write('Query\tTimeSeconds\n')
        patternf.write('Topology\tPID\tDefinition\n')
        for line in topof:
            topstring = line.strip().split('\t')[1]
            topo = literal_eval(topstring)
            pattern = Pattern(topo)
            run_pattern(target_link,pattern,url,countf,patternf,queryf,node_types,edge_types,atype,btype)

if __name__ == '__main__':
    go(rebuild=False)
