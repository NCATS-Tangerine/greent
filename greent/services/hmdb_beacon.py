import requests
import json
from greent.service import Service
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KNode
from greent import node_types

logger = LoggingUtil.init_logging (__name__)

class HMDB(Service):
    """ Access HMDB via the beacon """

    def __init__(self, context):
        super(HMDB, self).__init__("hmbd", context)
        self.concept_types = {node_types.DISEASE: 'disease',
                              node_types.PATHWAY: 'pathway',
                              node_types.DISEASE_OR_PHENOTYPE: 'disease',
                              node_types.GENETIC_CONDITION: 'disease',
                              node_types.GENE: 'protein',
                              node_types.DRUG: 'metabolite',
                              node_types.ANATOMY: 'gross anatomical structure'}

    def request_concept (self, concept, stype=None):
        #Without quotes around the keyword, this function treats space as a delimiter...
        keyword = Text.un_curie (concept.identifier)
        keyword = '"{0}"'.format (keyword) if ' ' in keyword else keyword
        if stype is None:
            url = '{0}/concepts?keywords={1}'.format (self.url, keyword)
        elif stype == node_types.DISEASE:
            url = '{0}/concepts?keywords={1}&semanticGroups=DISO'.format (self.url, keyword)
        else:
            url = '{0}/concepts?keywords={1}'.format (self.url, keyword)
        return requests.get (url).json ()


    def request_statement(self,input_identifier,node_type):
        url = '{self.url}/statements?s={input_identifier}&categories={self.concept_types[node_type]}'

