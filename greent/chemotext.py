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
        
    def query (self, query):
        return self.request (
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

class Chemotext(Neo4JREST):
    def __init__(self, url="http://chemotext.mml.unc.edu:7474"):
        super (Chemotext, self).__init__(url)

    def term_to_term (self, A, limit=100):
        return self.query (
            query="MATCH (d:Term)-[r1]-(a:Art)-[r2]-(t:Term) WHERE d.name='%s' RETURN d, r1, a, r2, t LIMIT %s" % (A, limit))

    def disease_name_to_drug_name (self, disease, limit=100):
        result = []
        response = self.query (
            query="MATCH (d:Term {type:'Disease', name: '%s' })-[r1]-(a:Art)-[r2]-(t:Term {isDrug:true}) RETURN d, r1, a, r2, t LIMIT %s" %
            (disease, limit))
        print (json.dumps (response, indent=2))
        for r in response['results'][0]['data']:
            result.append (r['row'][4]['name'])
        return result
    
def test ():
    chemotext = Chemotext ()
    #obj = chemotext.disease_to_drug ("Asthma")
    #print (json.dumps (obj, indent=2))
    
    obj = chemotext.term_to_term ("Asthma")
    print (json.dumps (obj, indent=2))
