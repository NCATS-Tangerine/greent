import argparse
import json
import os
import requests
import yaml
import shutil
try:
   from smartBag.grok import SemanticCrunch
except:
   print ("smartbag not in path. skipping import.")
from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode,KEdge,elements_to_json
from flask import Flask, jsonify, g, Response
from flasgger import Swagger
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

def get_associated_disease (name):
   """ Get identifiers for a disease name. """ 
   result = []
   obj = requests.get (f"https://solr.monarchinitiative.org/solr/search/select/?q=frequenc%20infections+%22{name}&rows=20&defType=edismax&hl=true&qt=standard&indent=on&wt=json&hl.simple.pre=%3Cem%20class=%22hilite%22%3E&hl.snippets=1&qf=synonym^1&qf=synonym_std^1&qf=synonym_kw^1&qf=synonym_eng^1").json()
   docs = obj.get('response',{}).get('docs',[])
   for doc in docs:
      if doc.get('prefix',None) in [ 'MONDO', 'DOID', 'OMIM' ]:
         result.append (doc.get('id_std', None))
   return result

def node2json (node):
   return {
      "identifier" : node.identifier,
      "type"       : f"blm:{node.node_type}",
      "id"         : id(node)
   } if node else None

def render_graph (blackboard):
   """ Turn a blackboard into json. Work towards a unique key for node. """
   edges = []
   nodes = {}
   for e in blackboard:
      if not e: # or not e.source_node or not e.target_node:
         continue
      if not 'stdprop' in e.properties:
         e.properties['stdprop'] = {}
      e.properties['stdprop']['subj'] = id(e.source_node)
      e.properties['stdprop']['obj'] = id(e.target_node)
      edges.append (e)
      #print (f"============> edge: {e}")
      #print (f"============> node : {e.source_node}")
      nodes[id(e.source_node)] = e.source_node
      nodes[id(e.target_node)] = e.target_node
   return {
      "edges" : [ elements_to_json(e) for e in blackboard ],
      "nodes" : [ node2json(n) for n in nodes.values () ]
   }

rosetta = None
def get_rosetta ():
   global rosetta
   if not rosetta:
      config = app.config['SWAGGER']['greent_conf']
      rosetta = Rosetta (debug=True, greentConf=config)
   return rosetta

@app.route('/cop/', methods=['GET'])
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

   rosetta = get_rosetta ()
   drug_id = rosetta.n2chem(drug)
   disease_id = get_associated_disease(disease)
   g = {}
   key = f"cop-drug({drug})-disease({disease})"
   g = rosetta.service_context.cache.get (key)
   if not g:
      blackboard = rosetta.get_knowledge_graph(**{
         "inputs" : {
            "type" : "chemical_substance",
            "values" : drug_id
         },
         "ends" : disease_id,
         "query" : """
         MATCH p=
         (c0:Concept {name: "chemical_substance" })--
         (c1:Concept {name: "gene" })--
         (c2:Concept {name: "biological_process" })--
         (c3:Concept {name: "cell" })--
         (c4:Concept {name: "anatomical_entity" })--
         (c5:Concept {name: "phenotypic_feature" })--
         (c6:Concept {name: "disease" })
         FOREACH (n in relationships(p) | SET n.marked = TRUE)
         WITH p,c0,c6
         MATCH q=(c0:Concept)-[*0..6 {marked:True}]->()<-[*0..6 {marked:True}]-(c6:Concept)
         WHERE p=q
         AND ALL( r in relationships(p) WHERE  EXISTS(r.op) )FOREACH (n in relationships(p) | SET n.marked = FALSE)
         RETURN p, EXTRACT( r in relationships(p) | startNode(r) )"""
      })
      g = render_graph(blackboard)
      rosetta.service_context.cache.set (key, g)
   return jsonify (g)

@app.route('/query/<inputs>/<query>')
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
         WHERE NONE (r IN relationships(p)
         WHERE type(r)="UNKNOWN"
         OR r.op is null)
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
   if '=' not in inputs:
      raise ValueError ("Inputs must be key value of concept=<comma separated ids>")   
   concept, items =inputs.split ("=")
   query = query.replace ("UNKNOWN", "'UNKNOWN'")
   blackboard = get_rosetta().construct_knowledge_graph(**{
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
   app.config['SWAGGER']['greent_conf'] = args.greent_conf = args.conf
   app.run(host='0.0.0.0', port=args.port, debug=True, threaded=True)
