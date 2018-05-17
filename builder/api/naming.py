import argparse
import glob
import json
import os
import re
import requests
import yaml
import shutil
from lru import LRU
from greent.graph_components import KNode,KEdge,elements_to_json
from greent.services.bionames import BioNames
from greent.servicecontext import ServiceContext
from flask import Flask, jsonify, g, Response
from flasgger import Swagger
app = Flask(__name__)

template = {
  "swagger": "2.0",
  "info": {
    "title": "Generic Name Resolution API",
    "description": "Generic facility aggregating bio-ontology lookup services to get ids based on natural language names.",
    "contact": {
      "responsibleOrganization": "renci.org",
      "responsibleDeveloper": "scox@renci.org",
      "email": "x@renci.org",
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
   'title': 'BioNames Service'
}

swagger = Swagger(app, template=template)

core = BioNames(ServiceContext.create_context ())
cache = LRU (1000) 

@app.route('/lookup/<q>/<concept>/')
def lookup (q, concept):
   """ Find ids by various methods.
   ---
   parameters:
     - name: q
       in: path
       type: string
       required: true
       default: aspirin
       description: "A text string. Can be a fragment."
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /lookup/{{ input }}/{{ input2 }}
     - name: concept
       in: path
       type: string
       required: false
       default: drug
       description: "A biolink-model concept name."
       x-valueType:
         - http://schema.org/string
       x-requestTemplate:
         - valueType: http://schema.org/string
           template: /is_a/{{ input }}/{{ input2 }}
   responses:
     200:
       description: ...
   """
   assert q, "A string must be entered as a query."
   key = f"{q}-{concept}"
   if key in cache:
       result = cache[key]
   else:
       result = core.lookup(q, concept)
       if len(q) > 3:
           cache[key] = result
   return jsonify (result)

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Rosetta Server')
   parser.add_argument('-p', '--port',  type=int, help='Port to run service on.', default=5000)
   parser.add_argument('-d', '--debug', help="Debug.", default=False)
   parser.add_argument('-c', '--conf',  help='GreenT config file to use.', default="greent.conf")
   args = parser.parse_args ()
   app.config['SWAGGER']['greent_conf'] = args.greent_conf = args.conf
   app.config['onto'] = {
       'config' : args.conf,
       'debug'  : args.debug
   }
   app.run(host='0.0.0.0', port=args.port, debug=True, threaded=True)
