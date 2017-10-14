import requests
import json

class Neo4JREST(object):
    def __init__(self, url):
        self.url = url
        self.query_endpoint = "{0}/db/data/transaction".format (self.url)
        self.schema_endpoint = "{0}/db/data/schema/".format (self.url)

    def request (self, url, obj):
        return requests.post (url = self.query_endpoint,
                             data = json.dumps (obj, indent=2),
                             headers={ "Content-Type" : "application/json" }).json ()
        
    def query (self, query, labels=None, node_properties=None):
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
        if node_properties != None:
            response = self.filter_nodes (response, labels, node_properties)
        return response
    
    def filter_nodes (self, response, labels=None, properties=['identifier']):
        nodes = []
        for r in response['results']:
            for d in r['data']:
                for n in d['graph']['nodes']:
                    if labels != None:
                        if any (map (lambda b : b in n['labels'], labels)):
                            obj = {}
                            for prop in properties:
                                obj[prop] = n['properties'][prop]
                            nodes.append (obj)
                for s in d['graph']['relationships']:
                    pass
        return nodes
