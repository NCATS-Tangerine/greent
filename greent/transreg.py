import json
import logging
import requests
import traceback
import re
import os
import yaml
from jinja2 import Template
from collections import defaultdict
from greent.util import LoggingUtil
from pprint import pprint
from reasoner.graph_components import KNode,KEdge
from jsonpath_rw import jsonpath, parse

logger = LoggingUtil.init_logging (__name__, level=logging.DEBUG)

def defaultdict_to_regular(d):
    """ Recursively convert a defaultdict to a dict. """
    if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regular(v) for k, v in d.items()}
    return d

class TranslatorRegistry:
    """ Interact with Translator services. """
    def __init__(self, url):
        """ Read the Translator Registry root document. For each listed API, read its
            metadata. Then consider each path, parameter, and output in detail, regisgtering
            template strings used for invoking each service. Create a mapping to facilitate lookups
            of invocation templates based on type pairs. Later, need semantics about what the 
            meaning of the transitions is. Revenge of the semantic web and stuff."""
        self.url = url
        self.punctuation = re.compile('[ \./:]+')

        # Use cached model
        registry_map = os.path.join (os.path.dirname (__file__), "transreg.yml")
        if os.path.exists (registry_map):
            with open (registry_map, "r") as stream:
                logger.debug ("Loading cached copy of translator registry config")
                self.op_map = yaml.load (stream.read ())
                return

        # Dynamically generate model
        self.op_map = defaultdict(lambda:defaultdict(lambda:defaultdict(None)))
        url = "{0}/API_LIST.yml".format (self.url)
        print (url)
        registry = yaml.load (requests.get (url).text)
        apis = {}
        for context in registry['APIs']:
            metadata = context['metadata']
            api_name = metadata.split (os.sep)[0].replace (" ","")
            logger.debug ("API: {}".format (api_name))
            api_url = "{0}/{1}".format (self.url, metadata)
            model = yaml.load (requests.get (api_url).text)
            servers = model.get('servers', [])
            server = None
            if isinstance(servers,list) and len(servers) > 0:
                server = servers[0]['url']
            paths = model.get('paths', {})
            for path in paths:
                obj = paths[path]
                logger.debug ("path: {}".format (path))
                get = obj['get'] if 'get' in obj else {}
                logger.debug ("get: {}".format (get))
                for parameters in get.get('parameters',{}):
                    logger.debug ("param: {}".format (parameters))
                    if 'x-valueType' in parameters:
                        values_in = parameters.get('x-requestTemplate',{})
                        logger.debug ("x-valueType: {}".format (values_in))
                        for v in values_in:
                            in_type = v['valueType']
                            x_template = v['template']
                            logger.debug ("in_type: {}".format (in_type))
                            for r in get.get('responses',{}).get('200',{}).get('x-responseValueType',{}):
                                out_type = r['valueType']
                                logger.debug ("out_type: {}".format (out_type))
                                logger.debug ("  --api> {0} in: {1} out: {2}".format (api_name, in_type, out_type))
                                self.op_map[api_name][in_type][out_type] = {
                                    "op"  : path,
                                    "get" : "{0}{1}".format (server, x_template)
                                }
        # Cache model
        vanilla_op_map = defaultdict_to_regular (self.op_map)
        with open (registry_map, "w") as stream:
            logger.debug ("Cache copy of registry map")
            yaml.dump (vanilla_op_map, stream, default_flow_style=False)

    def get (self, api_name, template, v, response_processor=None):
        """ Invoke a GET requests on the specified API with the template and object. """        
        if 'NCBIGene:' in template: # Need to better understand the intent and usage of this syntax.
            template = template.replace ('NCBIGene:','')
        url = Template (template).render (input=v.identifier)
        result = []
        try:
            logger.debug ("                   --get: {0}".format (url))
            response = requests.get (url).json ()
            if response_processor:
                vals = response_processor (response)
                result += [ ( KEdge(api_name, 'queried', response), KNode(v, self.abstract(v)) ) for v in vals ]
            for obj in response.get('objects',{}): # replace this, specific to biolink, with more general above approach.
                x_type = self.abstract (obj)
                result.append ( ( KEdge(api_name, 'queried', response), KNode(obj, x_type) ))
        except:
            pass
        return result
    
    def abstract(self, obj):
        x_type = '?'
        if ':' in obj:
            curie = obj.split (':')[0]
            type_m = {
                'HP'       : 'PH',
                'OMIM'     : 'D',
                'DOID'     : 'D',
                'UNIPROT:' : 'G',
                'NCBIGene' : 'G'
            }
            x_type = type_m[curie] if curie in type_m else '?'
            
    def human_name (self, iri):
        for prefix in [
                "http://identifiers.org/",
                "http://biothings.io/" ]:
            iri = iri.replace (prefix, "")
        return self.punctuation.sub ('', iri)

    def get_method_name (self, api, in_type, out_type):
        x_api = self.human_name (api)
        x_in_type = self.human_name (in_type)
        x_out_type = self.human_name (out_type)
        return "{0}__{1}_to_{2}".format (x_api, x_in_type, x_out_type)

    def diseaseontologyapi__doid_to_mesh_processor (self, response):
        jsonpath_expr = parse('xrefs[*]')
        r =  [ match.value for match in jsonpath_expr.find (response) if match.value.startswith ("MESH:") ]
        return r
    
    def add_method (self, cls, api, template, in_type, out_type):
        method_name = self.get_method_name (api, in_type, out_type)
        def new_method(self, v):
            """ A dynamically created method to perform a translation. If we can't find a response processor, for now, don't bother. """
            result_processor = getattr(self, "{0}_processor".format (method_name), None)
            return self.get (api, template, v, result_processor) if result_processor else []
        new_method.__doc__ = "convert from {0} to {1}".format (in_type, out_type)
        new_method.__name__ = method_name
        logger.debug ("adding method: {0}".format (new_method.__name__))
        setattr(cls, new_method.__name__, new_method)

    def get_subscriptions (self):
        transitions = []
        subscriptions = []
        for api in self.op_map:
            for in_type, in_params in self.op_map[api].items ():
                for out_type, out_vals in self.op_map[api][in_type].items ():
                    template = out_vals.get ("get")
                    operation = out_vals.get ("op")
                    if template:
                        self.add_method (TranslatorRegistry, api, template, in_type, out_type)
                        subscriptions.append ((
                            in_type,
                            out_type,
                            { "op" : self.get_method_name (api, in_type, out_type) }
                        ))
        return subscriptions
    
if __name__ == "__main__":
    treg = TranslatorRegistry ("https://raw.githubusercontent.com/NCATS-Tangerine/translator-api-registry/master")
    subscriptions = treg.generate_transition_methods ()
    #r = treg.call ('HGNC:6871', 'http://identifiers.org/ncbigene/', 'http://identifiers.org/ncbigene/')
    print (subscriptions)
    r = treg.mygeneinfo__ncbigene_to_wikipathways ("NCBIGene:10015")
    print (r)

