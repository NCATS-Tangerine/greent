import argparse
import glob
import json
import os
import re
import requests
import yaml
import shutil
from greent import node_types
from greent.graph_components import KNode,KEdge,elements_to_json
from greent.services.ontology import GenericOntology
from greent.servicecontext import ServiceContext
from flask import Flask, jsonify, g, Response
from flasgger import Swagger
app = Flask(__name__)

template = {
  "swagger": "2.0",
  "info": {
    "title": "Generic Ontology API",
    "description": "Generic facts about ontologies.",
    "contact": {
      "responsibleOrganization": "renci.org",
      "responsibleDeveloper": "scox@renci.org",
      "email": "x@renci.org",
      "url": "www.renci.org",
    },
    "termsOfService": "http://renci.org/terms",
    "version": "0.0.1"
  },
#  "basePath": "/onto/api",
  "schemes": [
    "http",
    "https"
  ]
}
app.config['SWAGGER'] = {
   'title': 'Ontology Service'
}

swagger = Swagger(app, template=template)

class Core:
    
    """ Core ontology services. """
    def __init__(self):
        self.onts = {}
        self.context = service_context = ServiceContext (
            config=app.config['SWAGGER']['greent_conf'])
        data_dir = app.config['onto']['data']
        data_pattern = os.path.join (data_dir, "*.obo")
        ontology_files = glob.glob (data_pattern)
        for f in ontology_files:
            print (f"loading {f}")
            file_name = os.path.basename (f)
            name = file_name.replace (".obo", "")
            self.onts[name] = GenericOntology(self.context, f) 
    def ont (self, curie):
        return self.onts[curie.lower()] if curie and curie.lower() in self.onts else None
    
core = None
def get_core (curie=None):
    global core
    if not core:
        print (f"initializing core")
        core = Core ()
    result = core
    if curie:
        if ":" in curie:
            curie = curie.split(":")[0]
        result = core.ont (curie)
    return result
     
@app.route('/is_a/<curie>/<ancestors>/')
def is_a (curie, ancestors):
   """ Determine ancestry.
   ---
   parameters:
     - name: curie
       in: path
       type: string
       required: true
       default: GO:2001317
       description: "An identifier from an ontology. eg, GO:2001317"
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /is_a/{{ input }}/{{ input2 }}
     - name: ancestors
       in: path
       type: array
       required: true
       default: GO:1901362
       items:
         type: string
       description: "A comma separated list of identifiers. eg, GO:1901362"
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /is_a/{{ input }}/{{ input2 }}
   responses:
     200:
       description: ...
   """
   assert curie, "An identifier must be supplied."
   assert isinstance(ancestors, str), "Ancestors must be one or more identifiers"
   ont = get_core (curie)
   return jsonify ({
       "is_a"      : ont.is_a(curie, ancestors),
       "id"        : curie,
       "ancestors" : ancestors
   })
     
@app.route('/label/<curie>/')
def label (curie):
   """ Get ontology term label by id.
   ---
   parameters:
     - name: curie
       in: path
       type: string
       required: true
       default: GO:2001317
       description: "An identifier from an ontology. eg, GO:2001317"
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /label/{{ input }}/
   responses:
     200:
       description: ...
   """
   core = get_core ()
   label = None
   for k, v in core.onts.items ():
       label = v.label (curie)
       if label:
           break
   return jsonify ({
       "label"     : label,
       "id"        : curie
   })

@app.route('/search/<pattern>/')
def search (pattern):
   """ Search for ids in an ontology based on a pattern, optionally a regular expression.
   ---
   parameters:
      - name: pattern
       in: path
       type: string
       required: true
       default: "kidney"
       description: "Pattern to search for. .*kojic.*"
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
            template: /search/{{ pattern }}/?regex={{ regex }}
     - name: regex
        in: query
       type: boolean
       required: true
       default: false
       description: Is the pattern a regular expession?
       x-valueType:
         - http://schema.org/boolean
       x-requestTemplate:
         - valueType: http://schema.org/boolean
            template: /search/{{ pattern }}/?regex={{ regex }}
   responses:
     200:
       description: ...
   """
    params = request.args
    regex = 'regex' in params and params['regex'] == 'true'
   core = get_core ()
    
    obo_map = {
        'chebi': 'chemical_substance',
        'doid': 'disease'
    }
    vals = []
    for name, ont in core.onts.items():
        new = ont.search(pattern, regex)
        for n in new:
            n['type'] = obo_map[name]
        vals.extend(new)
   return jsonify ({ "values" : vals })
     
@app.route('/xrefs/<curie>')
def xrefs (curie):
   """ Get external references to other ontologies from this id.
   ---
   parameters:
     - name: curie
       in: path
       type: string
       required: true
       default: "MONDO:0001106"
       description: "Curie designating an ontology. eg, GO:2001317"
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /xrefs/{{ curie }}/
   responses:
     200:
       description: ...
   """
   ont = get_core (curie)
   return jsonify ({
       "xrefs"     : [ x.split(' ')[0] if ' ' in x else x for x in ont.xrefs (curie) ]
   } if ont else {})


@app.route('/lookup/<curie>')
def lookup (curie):
   """ Get ids for which this curie is an external reference.
   ---
   parameters:
     - name: curie
       in: path
       type: string
       required: true
       default: "OMIM:143100"
       description: "Curie designating an external reference."
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /lookup/{{ curie }}/
   responses:
     200:
       description: ...
   """
   core = get_core ()
   return jsonify ({
       "refs" : [ ref for name, ont in core.onts.items() for ref in ont.lookup (curie) ]
   })
     
@app.route('/synonyms/<curie>/')
def synonyms (curie):
   """ Get synonym terms for the given curie.
   ---
   parameters:
     - name: curie
       in: path
       type: string
       required: true
       default: "GO:0000009"
       description: "Curie designating an ontology. eg, GO:0000009"
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /synonyms/{{ curie }}/
   responses:
     200:
       description: ...
   """
   result = []
   ont = get_core (curie)
   if ont:
       syns = ont.synonyms (curie)
       if syns:
           for syn in syns:
               result.append ({
                   "desc" : syn.desc,
                   "scope" : syn.scope,
                   "syn_type" : syn.syn_type.name if syn.syn_type else None,
                   "xref"     : syn.xref
               })
   return jsonify (result)

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Rosetta Server')
   parser.add_argument('-p', '--port',  type=int, help='Port to run service on.', default=5000)
   parser.add_argument('-d', '--debug', help="Debug.", default=False)
   parser.add_argument('-t', '--data',  help="Ontology data source.", default=".")
   parser.add_argument('-c', '--conf',  help='GreenT config file to use.', default="greent.conf")
   args = parser.parse_args ()
   app.config['SWAGGER']['greent_conf'] = args.greent_conf = args.conf
   app.config['onto'] = {
       'config' : args.conf,
       'data'   : args.data,
       'debug'  : args.debug
   }
   app.run(host='0.0.0.0', port=args.port, debug=True, threaded=True)
