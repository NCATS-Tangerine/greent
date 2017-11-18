import json
import logging
import requests
import traceback
import re
import os
import sys
import yaml
from jinja2 import Template
from collections import defaultdict
from collections import namedtuple
from greent.node_types import node_types
from greent.service import Service
from greent.service import ServiceContext
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import DataStructure
from pprint import pprint
from greent.graph_components import KNode,KEdge
from greent import node_types
from jsonpath_rw import jsonpath, parse
from greent.util import Text
from pyld import jsonld

logger = LoggingUtil.init_logging (__name__, level=logging.DEBUG)

def defaultdict_to_regular(d):
    """ Recursively convert a defaultdict to a dict. """
    if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regular(v) for k, v in d.items()}
    return d

class TranslatorRegistry(Service):
    """ Interact with Translator services. """
    def __init__(self, context):
        """ Read the Translator Registry root document. For each listed API, read its
            metadata. Then consider each path, parameter, and output in detail, regisgtering
            template strings used for invoking each service. Create a mapping to facilitate lookups
            of invocation templates based on type pairs. Later, need semantics about what the 
            meaning of the transitions is. Revenge of the semantic web and stuff."""
        super(TranslatorRegistry, self).__init__("transreg", context)
        self.verbose = True #False
        self.punctuation = re.compile('[ \./:]+')
        self.rosetta = None
        
        # Use cached model
        self.op_map = Resource.get_resource_obj ("transreg.yml", format='yaml')
        if isinstance (self.op_map, dict):
            logger.debug ("Loaded cached copy of translator registry config.")
            return
        
        # Dynamically generate model
        self.op_map = defaultdict(lambda:defaultdict(lambda:defaultdict(None)))
        url = "{0}/API_LIST.yml".format (self.url)
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
                            success_response = get.get('responses',{}).get('200',{})

                            json_ld_url = success_response.get('x-JSONLDContext',None)
                            json_ld = {}
                            if json_ld_url:
                                json_ld = requests.get(json_ld_url).json ()

                            for response_value in success_response.get('x-responseValueType',{}):
                                out_type = response_value['valueType']
                                logger.debug ("out_type: {}".format (out_type))
                                logger.debug ("  --api> {0} in: {1} out: {2}".format (api_name, in_type, out_type))
                                self.op_map[api_name][in_type][out_type] = {
                                    "op"       : path,
                                    "get_url"  : "{0}{1}".format (server, x_template),
                                    "out_type" : response_value.get('valueType', None),
                                    "obj_path" : response_value.get('path', None),
                                    "jsonld"   : json_ld
                                }
        # Cache model
        registry_map = os.path.join (os.path.dirname (__file__), "transreg.yml")
        vanilla_op_map = defaultdict_to_regular (self.op_map)
        with open (registry_map, "w") as stream:
            logger.debug ("Cache copy of registry map")
            yaml.dump (vanilla_op_map, stream, default_flow_style=False)

    def set_rosetta (self, rosetta):
        self.rosetta = rosetta
        
    def path_to_method_name (self, path):
        return path.replace ("{","").replace ("}","").replace ("?","/").replace ("=","_").replace ("/","_")
    
    def get_service_metadata (self, api_name, in_type, out_type):
        metadata = self.op_map.get(api_name,{}).get (in_type,{}).get (out_type,{})
        return DataStructure.to_named_tuple ('ServiceMetadata', metadata) if len(metadata) > 0 else None
    
    def get (self, api_name, v, method_metadata):
        """ Invoke a GET requests on the specified API for value v with the given metadata. """
        result = []
        try:
            """ Get the service metadata """
            service_metadata = self.get_service_metadata (api_name, method_metadata.in_type, method_metadata.out_type)
            logger.debug ("* Executing translator registry method: {0} in: {1} out: {2} template: {3} value: {4} ".format (
                api_name, method_metadata.in_type, method_metadata.out_type, service_metadata.get_url, v))

            """ Parameterize and execute the HTTP request. """
            url = Template (service_metadata.get_url).render (input=v)
            response = requests.get (url).json ()
            
            """ Expand the context with JSON-LD """
            jsonld_context = json.loads (json.dumps (service_metadata.jsonld),
                                         parse_float=lambda v : str (v))
            del jsonld_context['@context']['@version']
            expanded = jsonld.expand (
                response,
                {
                    "expandContext" : jsonld_context['@context']
                })

            """ Parse the JSON-LD expanded response with JSON-Path. """
            json_path_expr = parse (method_metadata.path)
            result_vals = [ match.value
                            for match in json_path_expr.find (expanded)
                            if match.value.startswith (method_metadata.out_type) ]

            """ Convert to curies. """
            out_curie = self.rosetta.to_curie (self.rosetta.unterminate (method_metadata.out_type))
            if out_curie:
                result_vals = [ r.replace (method_metadata.out_type, "{0}:".format (out_curie))
                                for r in result_vals ]
                
            """ Create graph edges and nodes to represent results. """
            node_type = self.abstract (method_metadata.out_type)
            for r in result_vals:
                if node_type != None:
                    result.append (
                        ( self.get_edge(response, predicate=method_metadata.predicate), KNode(r, node_type) )
                    )
        except Exception as e:
            traceback.print_exc ()
            exc_type, exc_value, exc_tb = sys.exc_info()
            exception_text = traceback.format_exception (exc_type, exc_value, exc_tb)
            logger.error (exception_text)
        return result
    
    def abstract (self, obj):
        """ Given a very specific type, translate it "up" to a robokop type. """
        x_type = None
        if obj.startswith ("http://"):
            type_m = {
                'http://identifiers.org/ncbigene/' : node_types.GENE
            }
            x_type = type_m[obj] if obj in type_m else None
        elif ':' in obj:
            curie = obj.split (':')[0]
            type_m = {
                'HP'       : node_types.PHENOTYPE,
                'OMIM'     : node_types.DISEASE,
                'DOID'     : node_types.DISEASE,
                'UNIPROT:' : node_types.GENE,
                'NCBIGene' : node_types.GENE
            }
            x_type = type_m[curie] if curie in type_m else None
        return x_type
    
    def human_name (self, iri):
        """ Replace a few things to simplify the text. """
        for prefix in [
                "http://identifiers.org/",
                "http://biothings.io/" ]:
            iri = iri.replace (prefix, "")
        return self.punctuation.sub ('', iri)

    def get_method_name (self, api, method_metadata):
        """ Build a normalized name for the method based on its input and output IRIs """
        x_api = self.human_name (api)
        in_curie = self.rosetta.to_curie (method_metadata.in_type)
        out_curie = self.rosetta.to_curie (method_metadata.out_type)
        return "{0}__{1}_{2}_{3}".format (
            x_api, method_metadata.in_curie, method_metadata.predicate,
            method_metadata.out_curie).lower ()

    def add_method (self, cls, api, method_metadata):
        """ Dynamically create a method on this object to invoke a particular API. """
        method_metadata.op = self.get_method_name (api, method_metadata)
        
        """ Create the new method. Pass its metadata along in each invocation. """
        def new_method(self, v):
            """ A dynamically created method to perform a translation. """
            return self.get (api, v, method_metadata)

        """ Add the method to this class. """
        new_method.__doc__ = "convert from {0} to {1}".format (method_metadata.in_type, method_metadata.out_type)
        new_method.__name__ = method_metadata.op
        setattr(cls, new_method.__name__, new_method)
        return method_metadata.op
    
    def get_subscriptions (self):
        """ Provide enough information to subscribe translator services as part of the Rosetta translation scheme.
        This involves passing back source type, destination type, semantic predicate,  and the method executing the transition.
        Rosetta only needs the name of the method since it will look up the actual method to dispatch dynamically. """
        rosetta_config = Resource.get_resource_obj ("rosetta.yml", format='yaml')
        semantics = rosetta_config['@translator-semantics']
        subscriptions = []
        """ Iterate over registry APIs. """
        for api in self.op_map:
            for in_type, in_params in self.op_map[api].items ():
                for out_type, out_vals in self.op_map[api][in_type].items ():
                    in_curie = self.rosetta.to_curie (in_type)
                    out_curie = self.rosetta.to_curie (out_type)
                    predicate = semantics.get (api,{}).get (in_curie,{}).get (out_curie,{}).get ("link", None)
                    path      = semantics.get (api,{}).get (in_curie,{}).get (out_curie,{}).get ("path", None)
                    method_metadata = MethodMetadata (
                        in_type   = in_type,
                        out_type  = out_type,
                        in_curie  = in_curie,
                        out_curie = out_curie,
                        predicate = predicate,
                        path      = path)
                    self.add_method (TranslatorRegistry, api, method_metadata)
                    subscriptions.append (method_metadata)
        return subscriptions

class MethodMetadata:
    """ Metadata about a method dynamically discovered from the translator registry and 
    attached to a TranslatorRegistry instance. """
    def __init__(self, in_type, out_type, in_curie, out_curie, predicate, path, op=None):
        self.in_type = in_type
        self.out_type = out_type
        self.in_curie = in_curie
        self.out_curie = out_curie
        self.predicate = predicate
        self.path = path
        self.op = op
        
if __name__ == "__main__":
    """ Load the registry """
    from rosetta import Rosetta
    treg = TranslatorRegistry (ServiceContext.create_context ())
    treg.set_rosetta (Rosetta ())
    
    """ Generate subscriptions """
    subscriptions = treg.get_subscriptions ()
    submap = {}
    for s in subscriptions:
        submap[s.op] = s
    pprint (submap)
    method_name = 'biolink__doid_associatedwithgene_ncbigene'
    meta = submap [method_name]
    m = getattr (treg, method_name)
    r = m ('DOID:2841')
    pprint (r)

