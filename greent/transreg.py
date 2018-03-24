import collections
import json
import logging
import requests
import traceback
import pytest
import re
import os
import sys
import yaml
import unittest
from jinja2 import Template
from collections import defaultdict
from collections import namedtuple
from greent.concept import Concept
from greent.concept import ConceptModel
from greent.identifiers import Identifiers
from greent.node_types import node_types
from greent.service import Service
from greent.service import ServiceContext
from greent.util import LoggingUtil
from greent.util import Resource
from greent.util import DataStructure
from greent.util import Text
from pprint import pprint
from greent.graph_components import KNode,KEdge
from greent import node_types
from jsonpath_rw import jsonpath, parse
from pyld import jsonld

logger = LoggingUtil.init_logging (__name__, level=logging.DEBUG)

# TODO: bind to biolink et al
node_type_map = {
    "gene"              : "G",
    "drug"              : "S",
    "process"           : "P",
    "cell"              : "C",
    "anatomy"           : "A",
    "phenotype"         : "P",
    "disease"           : "D",
    "genetic_condition" : "X",
    "pathway"           : "W",
}
def get_node_type (out_concept):
    out_concept = out_concept.lower ()
    node_type = node_type_map[out_concept] if out_concept in node_type_map else None
    node_type = node_types.type_codes [node_type]
    return node_type

def update_dict(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def defaultdict_to_regular(d):
    """ Recursively convert a defaultdict to a dict. """
    if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regular(v) for k, v in d.items()}
    return d
    
punctuation = re.compile('[ ?=\./:{}]+')
trailing_under = re.compile('\/$')

class MethodMetadata:
    """ Metadata about a method dynamically discovered from the translator registry and 
    attached to a TranslatorRegistry instance. """
    def __init__(self, api, path, in_types, in_concept, out_type, predicate, obj_path, out_concept=None, op=None):
        self.api = api
        self.path = path
        method_path = trailing_under.sub ('', self.path)
        self.in_types = in_types
        self.in_concept = in_concept
        self.out_type = out_type
        self.predicate = predicate
        self.obj_path = obj_path
        self.out_concept = out_concept
        self.op = punctuation.sub ('_', f"{self.api}_{method_path}")
    def __repr__(self):
        return f"in_concept: {self.in_concept} out_concept: {self.out_concept} in_types: {self.in_types} out_type: {self.out_type} path: {self.obj_path} op: {self.op}"

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
        self.concept_model = ConceptModel ("biolink-model")
        self.identifiers = Identifiers ()
        
        # Use cached model
        self.op_map = Resource.get_resource_obj (os.path.join ("conf", "transreg.yml"), format='yaml')
        if not isinstance (self.op_map, dict):
            logger.debug ("Loaded cached copy of translator registry config.")
            self._load_registry_map ()
            
    def _load_registry_map (self):
        """ Load the entire registry, parsing each specification. """
        self.op_map = defaultdict(lambda:defaultdict(lambda:defaultdict(None)))
        url = "{0}/API_LIST.yml".format (self.url)
        registry = yaml.load (requests.get (url).text)
        apis = {}
        for context in registry['APIs']:
            metadata = context['metadata']
            api_name = metadata.split (os.sep)[0].replace (" ","")

            """ For each API, load the specification... """
            logger.debug ("API: {}".format (api_name))
            api_url = "{0}/{1}".format (self.url, metadata)
            model = yaml.load (requests.get (api_url).text)
            #print (json.dumps (model, indent=2))

            """ Use local specification fragments as layers over the registry specifications.
            This allows us to extend the registry while working within the same general data structure. """
            layer_path = os.path.join ("registry", "layers", api_name, f"{api_name}.yaml")
            if os.path.exists (layer_path):
                with open (layer_path, "r") as stream:
                    layer = yaml.load (stream.read ())
                    model = update_dict (model, layer)
                     
            servers = model.get('servers', [])
            server = None
            if isinstance(servers,list) and len(servers) > 0:
                server = servers[0]['url']

            """ Process each path or operation in this API. """
            paths = model.get('paths', {})
            for path in paths:
                obj = paths[path]
                #logger.debug ("path: {}".format (path))
                get = obj['get'] if 'get' in obj else {}
                #logger.debug ("get: {}".format (get))

                """ Process a parameter of this operation. """
                for parameters in get.get('parameters',{}):
                    #logger.debug ("param: {}".format (parameters))
                    if 'x-valueType' in parameters:
                        values_in = parameters.get('x-requestTemplate',{})
                            
                        """ Parse response value settings for this operation. """
                        success_response = get.get('responses',{}).get('200',{})                            
                        json_ld_url = success_response.get('x-JSONLDContext',None)
                        json_ld = {}
                        if json_ld_url:
                            if json_ld_url.startswith ("http"):
                                json_ld = requests.get(json_ld_url).json ()
                            elif os.path.exists (json_ld_url):
                                """ Override the JSON-LD context supplied by the registry with a local file. """
                                with open (json_ld_url, "r") as stream:
                                    json_ld = json.loads (stream.read ())

                        """ Get concept types.
                                * Prefer explicitly curated metadata specifying a concept name.
                                * Guess the appropriate concept based on heuristic mappings
                                * TODO: Accept namespaced concpets from interoperable models (?)
                        """
                        all_input_types = [ v['valueType'] for v in values_in ]
                        in_concept = self.get_concept (parameters, all_input_types)
                        
                        all_out_types = [ response_value['valueType'] for response_value in success_response.get('x-responseValueType',{}) ]
                        out_concept = self.get_concept (success_response, all_out_types)
                        
                        for v in values_in: 
                            in_type = v['valueType']
                            x_template = v['template']
                            #logger.debug ("in_type: {}".format (in_type))
                            """ Create a record for each response scenario. """
                            """ TODO: We need to flexibly handle multiple response scenarios. Store multiple per operation and manage dynamically. """
                            for response_value in success_response.get('x-responseValueType',{}):
                                out_type = response_value['valueType']
                                #logger.debug ("out_type: {}".format (out_type))
                                #logger.debug ("  --api> {0} in: {1} out: {2}".format (api_name, in_type, out_type))
                                self.op_map[api_name][in_type][out_type] = {
                                    "op"           : path,
                                    "get_url"      : "{0}{1}".format (server, x_template),
                                    "in_concept"   : in_concept,
                                    "in_types"     : all_input_types,
                                    "out_type"     : out_type,
                                    "obj_path"     : response_value.get('path', None),
                                    "out_concept"  : out_concept,
                                    "jsonld"       : json_ld
                                }
                                #print (json.dumps (self.op_map[api_name][in_type][out_type], indent=2))
        # Cache model
        registry_map = os.path.join (os.path.dirname (__file__), "conf", "transreg.yml")
        vanilla_op_map = defaultdict_to_regular (self.op_map)
        with open (registry_map, "w") as stream:
            logger.debug ("Cache copy of registry map")
            yaml.dump (vanilla_op_map, stream, default_flow_style=False)
        return self.op_map

    def get_concept (self, context, identifiers):
        curies = list(filter (lambda v : v != None, [ self.identifiers.id2curie (v) for v in identifiers ]))
        concept = context.get ("x-concept", self.get_concept_name (curies))
        return concept

    def get_concept_name (self, curies):
        concept = self.concept_model.get_single_concept_by_prefixes (curies)
        return concept.name if concept else None
    
    def get_service_metadata (self, api_name, in_type, out_type):
        metadata = self.op_map.get(api_name,{}).get (in_type,{}).get (out_type,{})
        return DataStructure.to_named_tuple ('ServiceMetadata', metadata) if len(metadata) > 0 else None

    def new_edge (self, source, function, properties, source_node=None, target_node=None):
        edge = KEdge (source, function, properties)
        edge.source_node = source_node
        edge.target_node = target_node
        return edge
    
    def get (self, api_name, node, method_metadata):
        """ Invoke a GET requests on the specified API for value node with the given metadata. """
        result = []
        try:

            """ Find synonym in the input node of an appropriate input type for this operation. """
            input_arg = None
            input_type = None
            for synonym in node.synonyms:
                #print (f"synonym -> {synonym}")
                syn = self.identifiers.curie_instance2id (synonym)
                print (f"syn -> {syn}")
                print (f"syn -> {method_metadata.in_types}")
                for t in method_metadata.in_types:
                    if input_arg:
                        break
                    if t in syn:
                        input_arg = synonym.split (':')[1]
                        input_type = t
                        break

            """ Fail if no supplied synonym is of an appropriate type to make the call. """
            if not input_arg:
                raise ValueError (f"Node {node} contains no synonyms of type {method_metadata.in_types} required by operation {method_metadata.op}")

            """ Get the service metadata """
            service_metadata = self.get_service_metadata (api_name, input_type, method_metadata.out_type)
            logger.debug ("* Executing translator registry method: {0} in: {1} out: {2} template: {3} value: {4} ".format (
                api_name, input_type, method_metadata.out_type, service_metadata.get_url, node))

            """ Parameterize and execute the HTTP request. """
            url = Template (service_metadata.get_url).render (input=input_arg)
            response = requests.get (url).json ()
                #with open ("a.txt", "w") as stream:
                #    stream.write (json.dumps (response, indent=2))

            """ Expand the context with JSON-LD """
            jsonld_context = json.loads (json.dumps (service_metadata.jsonld),
                                         parse_float=lambda v : str (v))
            del jsonld_context['@context']['@version']
            expanded = jsonld.expand (
                response,
                {
                    "expandContext" : jsonld_context['@context']
                })

            """ Extract data from the returned JSON object. """
            """ TODO: Responses are complex. Figure out how to generalize
                         * Traversal of the response
                         * Decisions about how to create nodes and edges
                         * What to say about the semantic types of returned identifiers
            """
            print (json.dumps (expanded, indent=2))
            for obj in expanded:
                for predicate, v in obj.items ():
                    if isinstance (v, list):
                        for item in v:
                            val = item["@id"] if "@id" in item else None
                            if val:
                                curie = self.identifiers.instance2curie (val)
                                #print (f"val: {val} curie: {curie}")
                                out_concept = method_metadata.out_concept
                                node_type = get_node_type (out_concept)
                                if curie and node_type:
                                    #print (f" ------> node type {node_type} id {val} ")
                                    new_node = KNode(curie, node_type)
                                    result.append (
                                        ( self.new_edge(source=self.name,
                                                        function=predicate,
                                                        properties=response,
                                                        source_node = node,
                                                        target_node = new_node), new_node )
                                    )
                    
        except Exception as e:
            traceback.print_exc ()
            exc_type, exc_value, exc_tb = sys.exc_info()
            exception_text = traceback.format_exception (exc_type, exc_value, exc_tb)
            logger.error (exception_text)
        return result
    
    def add_method (self, cls, api, method_metadata):
        try:
            getattr (self, method_metadata.op)
        except:
            """ Create the new method. Pass its metadata along in each invocation. """
            def new_method(self, v):
                """ A dynamically created method to perform a translation. """
                return self.get (api, v, method_metadata)

            """ Add the method to this class. """
            new_method.__doc__ = "convert from {0} to {1}".format (method_metadata.in_types, method_metadata.out_type)
            new_method.__name__ = method_metadata.op
            setattr(cls, new_method.__name__, new_method)
    
    def get_subscriptions (self):
        """ Provide enough information to subscribe translator services as part of a translation scheme.
        This involves passing back source type, destination type, semantic predicate,  and the method executing the transition. """
        subscriptions = []
        """ Iterate over registry APIs. """
        for api in self.op_map:
            for in_type, in_params in self.op_map[api].items ():
                for out_type, out_vals in self.op_map[api][in_type].items ():
                    obj = json.dumps (out_vals, indent=2)
                    method_metadata = MethodMetadata (
                        api         = api,
                        path        = out_vals.get ("op"),
                        in_types    = out_vals.get ("in_types"),
                        in_concept  = out_vals.get ("in_concept"),
                        out_type    = out_type,
                        predicate   = "",
                        obj_path    = out_vals.get ("obj_path"),
                        out_concept = out_vals.get ("out_concept"))
                    self.add_method (TranslatorRegistry, api, method_metadata)
                    subscriptions.append (method_metadata)
        return subscriptions

treg = None

def test_build_registry ():
    """ Verify we can load the registry and generate subscriptions."""
    global treg
    if not treg:
        treg = TranslatorRegistry (ServiceContext.create_context ())
        subscriptions = treg.get_subscriptions ()
        submap = {}
        for s in subscriptions:
            submap[s.op] = s
        pprint (submap)
    return treg

def get_method (method_name):
    """ Verify we can load the registry and generate subscriptions."""    
    treg = test_build_registry ()
    return getattr (treg, method_name)

def test_biolink_disease_gene_omim ():
    """ Test we can build the model and call a biolink method with an OMIM ID """
    method = get_method ('biolink__bioentity_disease_disease_id_genes')
    node = KNode('OMIM:600807', 'Disease')
    node.synonyms.add ('DOID:2841')
    r = method (KNode('OMIM:600807', 'Disease')) # omim asthma
    pprint (r)

def test_biolink_disease_gene_doid ():
    """ Generate subscriptions """
    method = get_method ('biolink__bioentity_disease_disease_id_genes')
    r = method (KNode('DOID:2841', 'Disease'))
    pprint (r)

if __name__ == "__main__":
    test_biolink_disease_gene_omim ()
    test_biolink_disease_gene_doid ()
    
'''
1. [done] Remove rosetta as a dependency.
2. Implement curie - iri functionality in a common place.
3. Bind nodetypes to biolink-model
'''
