import requests
import json
from greent.service import Service
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KNode
from greent import node_types
from builder.question import LabeledID

logger = LoggingUtil.init_logging (__name__)

class HMDB(Service):
    """ Access HMDB via the beacon """

    def __init__(self, context):
        super(HMDB, self).__init__("hmdb", context)
        self.concepts_robo2hmdb = {node_types.DISEASE: 'disease',
                              node_types.PATHWAY: 'pathway',
                              node_types.DISEASE_OR_PHENOTYPE: 'disease',
                              node_types.GENETIC_CONDITION: 'disease',
                              node_types.GENE: 'protein',
                              node_types.DRUG: 'metabolite',
                              node_types.ANATOMY: 'gross anatomical structure'}
        #We're not auto-inverting because the map is not 1:1
        self.concepts_hmdb2robo = {'disease':node_types.DISEASE,
                                   'pathway':node_types.PATHWAY,
                                   'protein':node_types.GENE,
                                   'metabolite':node_types.DRUG,
                                   'gross anatomical structure':node_types.ANATOMY}
        #These are all the predicates you can get
        self.predicates = { "related to": "SEMMEDDB:ASSOCIATED_WITH",
                            "participates in": "RO:0000056",
                            "located in": "RO:0001025",
                            "interacts with": "RO:0002434"}

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

    def make_node(self,json_node):
        identifier = json_node['id']
        label = json_node['name']
        node_types = json_node['categories']
        if len(node_types) > 1:
            logger.warn("Multiple Node Types from HMDB")
        node_type = self.concepts_hmdb2robo[node_types[0]]
        return KNode(identifier, name=label, type=node_type)

    def make_predicate(self,json_node):
        if json_node['negated']:
            return None, True
        pred_id = self.predicates[json_node['relation']]
        pred_label = json_node['relation']
        return LabeledID(identifier=pred_id, label=pred_label), False

    def request_statement(self,input_identifier,node_type,fname):
        url = f'{self.url}/statements?s={input_identifier}&categories={self.concepts_robo2hmdb[node_type]}'
        raw_results = requests.get(url).json()
        results = []
        for triple in raw_results:
            subject_node = self.make_node(triple['subject'])
            predicate,negated = self.make_predicate(triple['predicate'])
            object_node = self.make_node(triple['object'])
            if negated:
                continue
            if subject_node.id == input_identifier:
                new_node = object_node
            elif object_node.id == input_identifier:
                new_node = subject_node
            else:
                raise Exception("Something has gone wrong in the identifiers")
            edge = self.create_edge(subject_node, object_node, f'hmdb.{fname}',
                    input_identifier, predicate, url = url)
            results.append( (edge, new_node) )
        return results

    def A_to_B(self,node,prefix,target_type,fname):
        input_ids = node.get_synonyms_by_prefix(prefix)
        results = []
        for iid in input_ids:
            en = self.request_statement(iid,target_type,fname)
            results.extend(en)
        return results

    def disease_to_metabolite(self,disease_node):
        return self.A_to_B(disease_node, 'UMLS', node_types.DRUG, 'disease_to_metabolite')

    def enzyme_to_metabolite(self,enzyme_node):
        return self.A_to_B(enzyme_node, 'UniProtKB', node_types.DRUG, 'enzyme_to_metabolite')

    def pathway_to_metabolite(self,pathway_node):
        return self.A_to_B(pathway_node, 'SMPDB', node_types.DRUG, 'enzyme_to_pathway')

    def metabolite_to_enzyme(self,metabolite_node):
        return self.A_to_B(metabolite_node, 'HMDB', node_types.GENE, 'metabolite_to_enzyme')
   
    def metabolite_to_disease(self,metabolite_node):
        return self.A_to_B(metabolite_node, 'HMDB', node_types.DISEASE, 'metabolite_to_disease')

    def metabolite_to_pathway(self,metabolite_node):
        return self.A_to_B(metabolite_node, 'HMDB', node_types.PATHWAY, 'metabolite_to_pathway')
