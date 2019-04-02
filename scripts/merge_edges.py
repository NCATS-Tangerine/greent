from neo4j.v1 import GraphDatabase
import yaml 
class DB:
    def __init__(self, url, auth):
        print(url, auth)
        self.driver =GraphDatabase.driver(url, auth= auth)
        self.url = url
        self.auth = auth 
        self.session = None
        self.init_session()

    def init_session(self):
        self.session = self.driver.session()
        print('DB session initialized')
    
    def close_session(self):
        if self.session:
            self.session.close()

    def __del__(self):
        print('closing DB session')

        self.close_session()
        
    def query(self, query):
        return self.session.run(query)


class Merger:
    def __init__ (self,url, auth):
        self.db = DB(url, auth)
    
    def get_edge_list(self, source_type, dest_type):
        query = f"""
        match (s:{source_type})-[r]->(e:{dest_type})
        with s,e,type(r) as typ, collect(r) as coll, size(collect(r)) as length
        with s,e, coll, length where length > 1
        return {{source_node: s, target_node: e, edges: coll}}
        """
        result = self.db.query(query)
        parsed_result = self.parse_result(result)
        
        return parsed_result
    
    def parse_result(self, result):
        results = {}
        for record in result:
            for r in record:
                if 'Concept' in r['source_node'].labels:
                    source_id = None
                    target_id = None
                    continue
                source_id =  r['source_node']['id']
                target_id =  r['target_node']['id']
                if source_id not in results:
                    results[source_id] = {}
                if target_id not in results[source_id]:
                    results[source_id][target_id] = []
            if source_id == None and target_id == None:
                continue
            results[source_id][target_id].extend(r['edges'])
        return results
    
    def has_duplicate_edges(self, edge_list):
        list_predicate_ids = [x['predicate_id'] for x in edge_list]
        return len([x for idx, x in enumerate(list_predicate_ids) if x in list_predicate_ids[idx+1:] ]) > 0
    
    def detect_dup(self, parsed_result):
        node_ids_with_dup = []
        for source_id in parsed_result:
            targets = parsed_result[source_id]
            for target in targets:
                edges = parsed_result[source_id][target]
                if self.has_duplicate_edges(edges):
                    node_ids_with_dup.append((source_id,target))
        return node_ids_with_dup
        
    def merge_similar_edges(self, edges_list):
        merges = {}
        for edge in edges_list:
            if 'op' in edge: 
                continue
            if edge['predicate_id'] not in merges:
                merges[edge['predicate_id']] = {'originals': []}
            merges[edge['predicate_id']]['originals'].append(edge)   
        removables = []
        for predicate_id in merges:
            if len(merges[predicate_id]['originals']) > 1:
                merges[predicate_id]['squash'] = self.squash_edges_list(predicate_id, merges[predicate_id]['originals'])
            else:
                removables.append(predicate_id)
        for r in removables:
            merges.pop(r, None)]
        return merges

        
    def convert_attributes_to_list(self, edge, dont_convert = ['predicate_id']):
        final_dict = {}
        if type(edge) != type({}):
            final_dict = {'id': [edge.id]}
        for prop in dont_convert:
            final_dict[prop] = edge[prop]
        for prop in edge :
            if prop not in dont_convert:
                final_dict[prop] = edge[prop] if type(edge[prop]) == type([]) else [edge[prop]]
        return final_dict

    def combine_two_edges(self, edge_1, edge_2):
        # prepare edge
        edge_1_dict = self.convert_attributes_to_list(edge_1)
        edge_2_dict = self.convert_attributes_to_list(edge_2)
        for prop in edge_1:
            if prop != 'publications' and prop != 'predicate_id':
                edge_1_dict[prop] += edge_2_dict[prop]
        return edge_1_dict
        
    def squash_edges_list(self, predicate_id, edge_list):
        # take the first one as a bucket, and add the others' data into the pockets
        bucket_edge = self.convert_attributes_to_list(edge_list[0])     
        for edge in edge_list[1:]:
            bucket_edge = self.combine_two_edges(bucket_edge, edge)
        return bucket_edge

    def delete_edge(self, edge_id, watch_mode):
        query = f"""
        Match (a)-[r]-(b) where id(r) = {edge_id} delete r
        """
        if watch_mode:
            print(query)
        else:
            self.db.query(query)

    def update_edge(self, edge_id, edge_data, watch_mode):
        query = f"""
        MATCH (a)-[r]-(b) where id(r) = {edge_id} 
        """ 
        query += '\n'.join([f"SET r.{prop} = {edge_data[prop]}" for prop in edge_data if prop != 'id'])
        if watch_mode:
          print(query)
        else:   
            self.db.query(query)
import sys, getopt

def printUsage():
    print (""" 
            Usage:
                merge_edges --username <neo4j user name> --password <password> --server <server addr>
        """)

def main(argv):
    server = None
    password = None
    username = None
    watch_mode = False
    try:
        opts, args = getopt.getopt(argv, "hu:p:s:w", ["username=","password=","server=","watch"])
    except getopt.GetoptError:
        printUsage()
    for opt, arg in opts:
        if opt == '-h':
            printUsage()
        elif opt in ('-u', '--username'):
            username = arg
        elif opt in ('-p', '--passsword'):
            password = arg
        elif opt in ('-s', '--server'):
            server = arg
        elif opt in ('-w','--watch'):
            watch_mode = True
            print('Going to watch mode. Queries will be displayed but not executed.')
        
    if server == None or password == None or username == None:
        printUsage()
        exit()

    merger = Merger(server, (username, password))
    rosetta_relations = {}
    with open('../greent/rosetta.yml') as ros_file:
        rosetta_relations = yaml.load(ros_file)
    pairs = []
    for x in rosetta_relations['@operators']:
        for y in rosetta_relations['@operators'][x]:
            pairs.append((x, y))
    pairs.append(('chemical_substance','disease'))
    pairs.append(('disease','chemical_substance'))
    parsed_results= {}
    edges_to_merge = {}
    i = 0
    for source, destination in pairs:
        i += 1
        print(f'Step - {i} / {len(pairs)}')
        print(f'getting mergables for {source} --> {destination}')

        edge_lists = merger.get_edge_list(source, destination)
        duplicates = merger.detect_dup(edge_lists)
        print (f'found {len(duplicates)}')
        for source_node_id, target_node_id in duplicates:
            # print(merger.merge_similar_edges(edge_lists[source_node_id][target_node_id]))
            bucket = merger.merge_similar_edges(edge_lists[source_node_id][target_node_id])
            if source_node_id not in edges_to_merge:
                edges_to_merge[source_node_id] = {}
            edges_to_merge[source_node_id][target_node_id] = {predicate_id: bucket[predicate_id]['squash'] for predicate_id in bucket}
            for predicate_id in bucket:
                ids = bucket[predicate_id]['squash']['id']
                merger.update_edge( ids[0],bucket[predicate_id]['squash'],watch_mode)
                for id in ids[1:]:
                    merger.delete_edge(id, watch_mode)


if __name__ == '__main__':
    main(sys.argv[1:])
    
                
