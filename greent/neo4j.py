import requests
import json
from greent.service import Service
from greent.service import ServiceContext

class Neo4JREST(Service):
    """ Speak to Neo4J via REST. """
    def __init__(self, name, context):
        super(Neo4JREST, self).__init__(name, context)
        self.cypher_uri = "{0}/db/data/cypher".format (self.url)
        self.query_endpoint = "{0}/db/data/transaction".format (self.url)
        self.schema_endpoint = "{0}/db/data/schema/".format (self.url)

    def request (self, url, obj):
        """ Make a request and return response. """
        return requests.post (url = url,
                             data = json.dumps (obj, indent=2),
                             headers={ "Content-Type" : "application/json" }).json ()
        
    def query (self, query, labels=None, node_properties=None, kinds=[ 'node' ]):
        """ Format a query. """
        response = self.request (
            url = self.query_endpoint,
            obj = {
                "statements": [
                    {
                        "statement": query,
                        "resultDataContents": [
                            "row",
                            "graph"
                        ],
                        "includeStats": True
                }
                ]
            })
        #print (json.dumps (response, indent=2))
        if node_properties or labels:
            response = self.filter_nodes (response, labels, node_properties, kinds)
        return response

    def execute_cypher (self, statement):
        response = requests.post (
            url = self.cypher_uri,
            data = json.dumps({
                "statements" : [{
                    "statement" : statement
                }]
            }),
            headers = { "Content-Type" : "application/json" })

    def filter_nodes (self, response, labels=None, properties=['identifier'], kinds=['node']):
        nodes = []
        relationships = []
        for r in response.get('results',[]):
            for d in r.get('data',[]):
                print ("data---------{}".format (json.dumps (d, indent=2)))
                print ("d2--------> {}".format (d.get('graph',{}).get('nodes',[])))
                d2 = d.get('graph',{}).get('nodes',[])
                print ("kinds: {}".format (kinds))
                if kinds == None or 'node' in kinds:
                    print ("--------> {}".format (d.get('graph',{}).get('nodes',[])))
                    for n in d2: #d.get('graph',{}).get('nodes'):
                        print("_______________GOT ONE")
                        print (json.dumps (n, indent=2))
                        if labels != None:
                            if any (map (lambda b : b in n['labels'], labels)):
                                print (labels)
                                if properties:
                                    obj = {}
                                    for prop in properties:
                                        obj[prop] = n['properties'][prop]
                                    nodes.append (obj)
                                else:
                                    print ("-----------")
                                    nodes.append (n['properties'])
                if 'relationships' in kinds:
                    for r in d['graph']['relationships']:
                        relationships.append (r)
        return nodes
