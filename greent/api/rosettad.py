import argparse
import json
import ndex2
import networkx as nx
import os
import requests
import shutil
import yaml
from builder.builder import KnowledgeGraph
from builder.knowledgeQuery import KnowledgeQuery
from flasgger import Swagger
from flask import Flask, jsonify, g, Response
from greent import node_types
from greent.graph_components import KNode,KEdge
from greent.rosetta import Rosetta
from greent.userquery import UserQuery
from ndex2 import create_nice_cx_from_networkx
from ndex2.client import Ndex2

try:
   from smartBag.grok import SemanticCrunch
except:
   print ("smartbag not in path. skipping import.")

app = Flask(__name__)

template = {
  "swagger": "2.0",
  "info": {
    "title": "Rosetta",
    "description": "A Knowledge Map API",
    "contact": {
      "responsibleOrganization": "renci.org",
      "responsibleDeveloper": "scox@renci.org",
      "email": "scox@renci.org",
      "url": "www.renci.org",
    },
    "termsOfService": "http://renci.org/terms",
    "version": "0.0.1"
  },
  "schemes": [
    "http",
    "https"
  ]
}
app.config['SWAGGER'] = {
   'title': 'Rosetta Service',
   'bag_source' : '/.'
}

swagger = Swagger(app, template=template)

class NDExSession:
   """ An interface to the NDEx network catalog. """ 
   def __init__(self, account, password, uri="http://public.ndexbio.org"):
      self.session = None
      try:
         self.session = ndex2.client.Ndex2 (uri, account, password)
         self.session.update_status()
         networks = self.session.status.get("networkCount")
         users = self.session.status.get("userCount")
         groups = self.session.status.get("groupCount")
         print(f"session: networks: {networks} users: {users} groups: {groups}")
      except Exception as inst:
         print(f"Could not access account {account}")
         raise inst
      
   def save_nx_graph (self, graph):
      """ Save a networkx graph to NDEx. """
      nice_cx = create_nice_cx_from_networkx (graph)
      print (f" connected: {nx.is_connected(graph.to_undirected())} edges: {len(graph.edges())} nodes: {len(graph.nodes())}")
      print (nice_cx)
      self.session.save_new_network (nice_cx)

def node2json (node):
   """ Serialize a node as json. """
   return {
      "id" : node.identifier,
      "type"       : f"blm:{node.node_type}",
   } if node else None

def edge2json(e):
   """ Serialize an edge as json. """
   edge = e[2]['object']
   return {
      "ctime"  : edge.ctime,
      "sub"    : edge.source_node.identifier,
      "prd"    : edge.predicate_id,
      "stdprd" : edge.standard_predicate_id,
      "obj"    : edge.target_node.identifier,
      "pubs"   : edge.publications
   }

def render_graph (blackboard):
   """ Turn a blackboard into json. Work towards a unique key for node. """
   nodes = {}
   for e in blackboard:
      if not e:
         continue
      nodes[id(e.source_node)] = e.source_node
      nodes[id(e.target_node)] = e.target_node
   return {
      "edges" : [ edge2json(e) for e in blackboard ],
      "nodes" : [ node2json(n) for n in nodes.values () ]
   }

def validate_cypher(query):
   """ Reject cypher we don't want to execute. """
   assert query is not None, "Valid query required."
   query_lower = query.lower ()
   if 'delete' in query_lower or 'detach' in query_lower or 'create' in query_lower:
      raise ValueError ("not")

class Gamma:
   """ A high level interface to the system including knowledge map, cache, reasoner, and NDEx. """
   def __init__(self):
      config = app.config['SWAGGER']['greent_conf']
      self.rosetta = Rosetta (debug=True, greentConf=config)
      self.knowledge = KnowledgeQuery ()
      self.ndex = None
      ndex_creds = "~/.ndex"
      if os.path.exists (ndex_creds):
         with open(ndex_creds, "r") as stream:
            ndex_creds = json.loads (stream.read ())
            self.ndex = NDExSession (ndex_creds['username'],
                                     ndex_creds['password'])
            
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
      
   def create_key (self, kind, path):
      """ create consistent cache keys, cleaning special characters. """
      joined_path = "/".join (path)
      return f"{kind}-{joined_path}".\
         replace (" ","_").\
         replace(",","_").\
         replace("'","").\
         replace('"',"")

gamma = None
def get_gamma ():
   global gamma
   if not gamma:
      gamma = Gamma ()
   return gamma

@app.route('/cop/<drug>/<disease>/', methods=['GET'])
def cop (drug="imatinib", disease="asthma"):
   """ Get service metadata 
   ---
   parameters:
     - name: drug
       in: path
       type: string
       required: false
       default: imatinib
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /query?drug={{ input }}
     - name: disease
       in: path
       type: string
       required: false
       default: asthma
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /query?disease={{ input }}
   responses:
     200:
       description: ...
   """
   gamma = get_gamma ()
   key = gamma.create_key ('cop', [drug, disease])
   graph = gamma.rosetta.service_context.cache.get (key)   
   if not graph:
      disease_ids = gamma.get_disease_ids (disease, filters=['MONDO'])
      print (f"DID: {disease_ids}")
      query = gamma.knowledge.create_query(
         start_name   = drug,
         start_type   = node_types.DRUG,
         end_name     = disease,
         end_type     = node_types.DISEASE, 
         two_sided    = True,
         intermediate = [
            { "type" : node_types.GENE,      "min_path_length" : 1, "max_path_length" : 1 },
            { "type" : node_types.PROCESS,   "min_path_length" : 1, "max_path_length" : 1 },
            { "type" : node_types.CELL,      "min_path_length" : 1, "max_path_length" : 1 },
            { "type" : node_types.ANATOMY,   "min_path_length" : 1, "max_path_length" : 1 },
            { "type" : node_types.PHENOTYPE, "min_path_length" : 1, "max_path_length" : 1 }
         ],
         end_values   = disease_ids)
      graph = gamma.knowledge.query (query, key)
      graph = {
         "nodes" : [ node2json(n) for n in graph.graph.nodes () ],
         "edges" : [ edge2json(e) for e in graph.graph.edges (data=True) ]
      }
      gamma.rosetta.service_context.cache.set (key, graph)
      if gamma.ndex:
         gamma.ndex.save_nx_graph (graph.graph)
   return jsonify (graph)

@app.route('/query/<inputs>/<query>/', methods=['GET'])
def query (inputs, query):
   """ Get service metadata 
   ---
   parameters:
     - name: inputs
       in: path
       type: string
       required: true
       default: drug=MESH:D000068877,DRUGBANK:DB00619
       description: A key value pair where the key is a biolink-model concept and the value is a comma separated list of curies. eg, concept=curie:id\[,curie:id\]
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /query?inputs={{ input }}
     - name: query
       in: path
       type: array
       required: true
       items:
         type: string
       description: A cypher query over the biolink-model concept space returning a shortest path.
       default: >
         MATCH (a:drug),(b:pathway), p = allShortestPaths((a)-[*]->(b))
         WHERE NONE (r IN relationships(p) WHERE type(r)="UNKNOWN" OR r.op is null) and 
         a:Concept and b:Concept
         RETURN p
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /query?inputs={{ input }}&query={{ query }}
   responses:
     200:
       description: ...
   """

   """ Validate input ids structure is <concept>=<id>[,<id>]* """
   validate_cypher (query)
   if '=' not in inputs:
      raise ValueError ("Inputs must be key value of concept=<comma separated ids>")
   concept, items =inputs.split ("=")
   gamma = get_gamma ()
   blackboard = gamma.rosetta.construct_knowledge_graph(**{
      "inputs" : {
         concept : items.split (",")
      },
      "query"  : query
   })
   return jsonify(render_graph(blackboard))

'''
@app.route('/smartbag/compile/<bag_url>/')
def smartbag_compile (bag_url):
   """ Given a smartBag URL, fetch the bag and compile it to a smartAPI.
   ---
   parameters:
     - name: bag_url
       in: path
       type: string
       required: true
       x-valueType:
         - http://schema.org/url
       x-requestTemplate:
         - valueType: http://schema.org/url
           template: /url={{ bag_url }}
   responses:
     200:
       x-responseValueType:
         - path: x.y.z
           valueType: http://x.y/z
       x-JSONLDContext: /x.jsonld
   """
   bag_source = app.config['SWAGGER']['bag_source']
   bag_url = f"{bag_source}/{bag_url}"
   print (bag_url)
   bag_archive_file = bag_url.split ("/")[-1]
   print (f"bag archive: {bag_archive_file}")
   
   bag_base_name = bag_archive_file.replace (".tgz", "").replace (".zip", "")
   out_dir = os.path.join ("smartbag", "work", bag_base_name)
   if os.path.exists (out_dir):
      shutil.rmtree (out_dir)
   os.makedirs (out_dir)
   bag_archive_file_fq = os.path.join (out_dir, bag_archive_file)
   if bag_url.startswith ("http"):
      r = requests.get (bag_url, stream=True)
      with open(bag_archive_file_fq, 'wb') as outf:
         for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
               outf.write(chunk)
   else:
      shutil.copyfile (bag_url, bag_archive_file_fq)
   manifest = SemanticCrunch.generate_smartapi(
      bag=bag_archive_file_fq,
      output_dir=out_dir,
      title="TODO-Title")
   return jsonify(manifest)
'''

if __name__ == "__main__":
   print ("""
      ____                         __    __                
     / __ \  ____    _____  ___   / /_  / /_  ____ _       
    / /_/ / / __ \  / ___/ / _ \ / __/ / __/ / __ `/       
   / _, _/ / /_/ / (__  ) /  __// /_  / /_  / /_/ /        version 0.0.1
  /_/ |_|  \____/ /____/  \___/ \__/  \__/  \__,_/  
      
   """)                                                  
   parser = argparse.ArgumentParser(description='Rosetta Server')
   parser.add_argument('-s', '--bag-source', help='Filesystem path or URL serving bags.', default='.')
   parser.add_argument('-p', '--port', type=int, help='Port to run service on.', default=None)
   parser.add_argument('-c', '--conf', help='GreenT config file to use.', default=None)
   args = parser.parse_args ()
   app.config['SWAGGER']['bag_source'] = args.bag_source
   app.config['SWAGGER']['greent_conf'] = args.conf
   app.run(host='0.0.0.0', port=args.port, debug=True, threaded=True)


'''
sudo su - evryscope -c "\
source /projects/stars/translator/app/rosettaVenv/bin/activate; \
cd /projects/stars/app/robokop-interfaces; \
PYTHONPATH=/projects/stars/app/robokop-interfaces gunicorn \
     --workers 10 \
     --timeout 3000 \
     --bind 0.0.0.0:5000 greent.api.server:app "
'''
