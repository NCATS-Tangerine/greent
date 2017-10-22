import json
import requests
import os
import yaml
from jinja2 import Template
from collections import defaultdict
from greent.util import LoggingUtil
from pprint import pprint
from reasoner.graph_components import KNode,KEdge

logger = LoggingUtil.init_logging (__name__)

class TranslatorRegistry:
    
    def __init__(self, url):
        self.url = url
        self.op_map = defaultdict(lambda:defaultdict(lambda:defaultdict(None)))
        url = "{0}/API_LIST.yml".format (self.url)
        print (url)
        registry = yaml.load (requests.get (url).text)
        apis = {}
        for context in registry['APIs']:
            metadata = context['metadata']
            api_name = metadata.split (os.sep)[0]
#            if api_name != 'biolink':
#                continue
            logger.debug ("API: {}".format (api_name))
            api_url = "{0}/{1}".format (self.url, metadata)
            model = yaml.load (requests.get (api_url).text)
            #pprint (model)
            servers = model.get('servers', [])
            server = None
            if isinstance(servers,list) and len(servers) > 0:
                server = servers[0]['url']
            paths = model.get('paths', {})
            for path in paths:
                obj = paths[path]
                #print ("path: {}".format (path))
                get = obj['get'] if 'get' in obj else {}
                #print ("get: {}".format (get))
                for parameters in get.get('parameters',{}):
                    #print ("param: {}".format (parameters))
                    if 'x-valueType' in parameters:
                        values_in = parameters.get('x-requestTemplate',{})
                        #print ("x-valueType: {}".format (values_in))
                        for v in values_in:
                            in_type = v['valueType']
                            x_template = v['template']
                            #print ("in_type: {}".format (in_type))
                            for r in get.get('responses',{}).get('200',{}).get('x-responseValueType',{}):
                                out_type = r['valueType']
                                #print ("out_type: {}".format (out_type))
                                logger.debug ("  --api> {0} in: {1} out: {2}".format (api_name, in_type, out_type))
                                self.op_map[api_name][in_type][out_type] = {
                                    "get" : "{0}{1}".format (server, x_template),
                                    "op"  : lambda v: self.get ("{0}{1}".format (server, x_template), v)
                                }
        #pprint (dict(self.op_map))

    def call (self, obj, in_type, out_type):
        result = []
        for api in self.op_map:
            logger.debug ("considering api: {}".format (api))
            get = self.op_map.get(api,{}).get(in_type,{}).get(out_type,{}).get('get')
            if get:
                try:
                    logger.debug ("   calling {0} {1} {2}".format (obj, in_type, out_type))
                    result += self.get (api, get, obj)
                except:
                    pass
        return result
    
    def get (self, api_name, template, v):
        if 'NCBIGene:' in template:
            template = template.replace ('NCBIGene:','')
        url = Template (template).render (input=v)
        print ("url: %s" % url)
        result = []
        try:
            response = requests.get (url).json ()
            #logger.debug (print (json.dumps (response, indent=2)))
            for obj in response.get('objects',{}):
                x_type = '?'
                if ':' in obj:
                    curie = obj.split (':')[0]
                    type_m = {
                        'HP'       : 'PH',
                        'OMIM'     : 'D',
                        'DOID'     : 'D',
                        'UNIPROT:' : 'G'
                    }
                    x_type = type_m[curie] if curie in type_m else '?'
                result.append ( ( KEdge(api_name, 'queried', response), KNode(obj, x_type) ))
        except:
            pass
        return result
    
if __name__ == "__main__":
    treg = TranslatorRegistry ("https://raw.githubusercontent.com/NCATS-Tangerine/translator-api-registry/master")
    r = treg.call ('HGNC:6871', 'http://identifiers.org/ncbigene/', 'http://identifiers.org/ncbigene/')
    print (r)
