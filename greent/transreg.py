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
from greent.service import Service
from greent.service import ServiceContext
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import DataStructure
from pprint import pprint
from reasoner.graph_components import KNode,KEdge
from jsonpath_rw import jsonpath, parse
from greent.util import Text

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

        # Use cached model
        self.op_map = Resource.get_resource_obj ("transreg.yml", format='yaml')
        if isinstance (self.op_map, dict):
            logger.debug ("Loaded cached copy of translator registry config.")
            return
        '''
        registry_map = os.path.join (os.path.dirname (__file__), "transreg.yml")
        if os.path.exists (registry_map):
            with open (registry_map, "r") as stream:
                logger.debug ("Loading cached copy of translator registry config")
                self.op_map = yaml.load (stream.read ())
                return
        '''
        
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
        vanilla_op_map = defaultdict_to_regular (self.op_map)
        with open (registry_map, "w") as stream:
            logger.debug ("Cache copy of registry map")
            yaml.dump (vanilla_op_map, stream, default_flow_style=False)

    def path_to_method_name (self, path):
        return path.replace ("{","").replace ("}","").replace ("?","/").replace ("=","_").replace ("/","_")
    
#    def to_named_tuple (self, type_name, d):
#        return namedtuple(type_name, d.keys())(**d)
    def get_service_metadata (self, api_name, in_type, out_type):
        metadata = self.op_map.get(api_name,{}).get (in_type,{}).get (out_type,{})
        return DataStructure.to_named_tuple ('ServiceMetadata', metadata) if len(metadata) > 0 else None #self.to_named_tuple ('ServiceMetadata', metadata) if len(metadata) > 0 else None
    
    def get (self, api_name, in_type, out_type, v, method_name):
        """ Invoke a GET requests on the specified API with the template and object. """
        result = []
        if not api_name in [ 'biolink' ]:
            return result
        try:
            service_metadata = self.get_service_metadata (api_name, in_type, out_type)
            logger.debug ("* Executing translator registry method: {0} in: {1} out: {2} template: {3} value: {4} ".format (
                api_name, in_type, out_type, service_metadata.get_url, v.identifier))

            """ Build and run the request. """
            url = Template (service_metadata.get_url).render (input=v.identifier)
            logger.debug ("                   translatR:get: {0}".format (url))
            response = requests.get (url).json ()
            
            """ Parse the response. """
            path_components = service_metadata.obj_path.split ('.')
            level = response
            for component in path_components:
                item = level.get (component, None)
                level = item
                print ("-------------------> item: {} {}".format (component, item))
            predicate = "{0}.{1}/{2}".format (api_name, method_name, path_components)
            result = [ ( self.get_edge(response, predicate=path_components), KNode(level, self.abstract(out_type)) ) ]
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            exception_text = traceback.format_exception (exc_type, exc_value, exc_tb)
            #logger.error (exception_text)
        return result
    
    def abstract(self, obj):
        """ Given a very specific type, translate it "up" to a robokop type. """
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
        """ Replace a few things to simplify the text. """
        for prefix in [
                "http://identifiers.org/",
                "http://biothings.io/" ]:
            iri = iri.replace (prefix, "")
        return self.punctuation.sub ('', iri)

    def get_method_name (self, api, in_type, out_type):
        """ Build a normalized name for the method based on its input and output IRIs """
        x_api = self.human_name (api)
        x_in_type = self.human_name (in_type)
        x_out_type = self.human_name (out_type)
        return "{0}__{1}_to_{2}".format (x_api, x_in_type, x_out_type)

    def add_method (self, cls, api, in_type, out_type):
        """ Dynamically create a method for this API to do a get.
        For now, if there's no response processor configured, we just don't bother calling the API. """

        # Construct an API name.
        method_name = self.get_method_name (api, in_type, out_type)

        # Define the new method.
        def new_method(self, v):
            """ A dynamically created method to perform a translation. If we can't find a response processor, for now, don't bother. """
            return self.get (api, in_type, out_type, v, method_name)

        # Now add the method to this class. 
        new_method.__doc__ = "convert from {0} to {1}".format (in_type, out_type)
        new_method.__name__ = method_name
        setattr(cls, new_method.__name__, new_method)
        return method_name

    def get_subscriptions (self):
        """ Provide enough information to subscribe translator services as part of the Rosetta translation scheme.
        This involves passing back source type, destination type, and the method executing the transition.
        Rosetta only needs the name of the method since it will look up the actual method to dispatch to dynamically. """
        rosetta_config = Resource.get_resource_obj ("rosetta.yml", format='yaml')
        semantics = rosetta_config['@translator-semantics']
        subscriptions = []
        for api in self.op_map:
            for in_type, in_params in self.op_map[api].items ():
                for out_type, out_vals in self.op_map[api][in_type].items ():
                    predicate = semantics.get (api,{}).get (in_type,{}).get (out_type,None)
                    #if not predicate:
                    #    predicate = '*-missing-*'
                    subscriptions.append ((
                        in_type,
                        out_type,
                        {
                            "link" : predicate,
                            "op"   : self.add_method (TranslatorRegistry, api, in_type, out_type)
                        }
                    ))
        return subscriptions
    
if __name__ == "__main__":
    """ Load the registry """
    treg = TranslatorRegistry (ServiceContext.create_context ())

    """ Generate subscriptions """
    subscriptions = treg.get_subscriptions ()

    r = treg.myvariantinfo__uniprot_to_hgvs(KNode ('UNIPROT:AKT1','G'))
    pprint (r)

