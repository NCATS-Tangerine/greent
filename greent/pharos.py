import requests
import json

class Pharos(object):
    def __init__(self, url="https://pharos.nih.gov/idg/api/v1"):
        self.url = url
    def request (self, url):
        return requests.get (url).json ()
#            url = url, 
#            headers={ "Content-Type" : "application/json" }).json ()
    def query (self, query):
        return self.request ("{0}/{1}".format (self.url, query))
    def target_to_id (self, target):
        target_map = self.query (query="targets/{0}".format (target))
        return target_map["id"]
    def target_to_disease (self, target_sym):
        target_id = self.target_to_id (target_sym)
        links = self.query (query="targets({0})/links".format (target_id))
        result = []
        for k in links:
            #print (k['id'])
            if k['kind'] == "ix.idg.models.Disease":
                for p in k['properties']:
                    #print (p)
                    if p['label'] == 'IDG Disease':
                        result.append (p['term'])
        for r in result:
            print (r)
        return result
    
        #https://pharos.nih.gov/idg/api/v1/targets(15633)/links
    def disease_map (self, disease_id):
        return self.query (query="diseases({0})".format (disesase_id))

def test ():
#    with open("pharos.txt", "r") as stream:
#        obj = json.loads (stream.read ())
#        print (json.dumps (obj, indent=2))        
    pharos = Pharos ()
    diseases = pharos.target_to_disease ("CACNA1A")
    print (diseases)
