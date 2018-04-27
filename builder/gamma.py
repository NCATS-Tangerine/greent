import json
import os
import requests
from builder.knowledgeQuery import KnowledgeQuery
from greent.services.ndex import NDEx
from greent.rosetta import Rosetta

class Gamma:
    
   """ A high level interface to the system including knowledge map, cache, reasoner, and NDEx. """
   def __init__(self, config="greent.conf", debug=False):
      self.rosetta = Rosetta (debug=debug, greentConf=config)
      self.knowledge = KnowledgeQuery ()
      self.ndex = None
      ndex_creds = os.path.expanduser("~/.ndex")
      if os.path.exists (ndex_creds):
         with open(ndex_creds, "r") as stream:
            ndex_creds_obj = json.loads (stream.read ())
            print (f"connecting to ndex as {ndex_creds_obj['username']}")
            self.ndex = NDEx (ndex_creds_obj['username'],
                              ndex_creds_obj['password'])
            
   def get_disease_ids(self,disease, filters=[]):
      """ Resolve names to identifiers. """
      obj = requests.get (f"https://bionames.renci.org/lookup/{disease}/disease/").json ()
      results = []
      for n in obj:
         an_id = n['id']
         if len(filters) > 0:
            for f in filters:
               if an_id.startswith(f"{f}:"):
                  results.append (an_id)
         else:
            results.append (an_id)
      return results

   def publish (self, key, graph):
       if self.ndex:
           self.ndex.save_nx_graph (key, graph)
           
   def create_key (self, kind, path):
      """ create consistent cache keys, cleaning special characters. """
      joined_path = "/".join (path)
      return f"{kind}-{joined_path}".\
         replace (" ","_").\
         replace(",","_").\
         replace("'","").\
         replace('"',"")

