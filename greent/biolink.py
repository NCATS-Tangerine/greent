import requests
import json
import urllib
from greent.service import Service
from greent.service import ServiceContext
from greent.mondo import Mondo
from greent.util import Text
from reasoner.graph_components import KNode,KEdge
from reasoner import node_types
import logging

class Biolink(Service):
    """ Preliminary interface to Biolink. Will move to automated Translator Registry invocation over time. """
    def __init__(self, context):
        super(Biolink, self).__init__("biolink", context)
        self.checker = Mondo(ServiceContext.create_context ())
    def gene_get_disease(self, gene_node):
        """Given a gene specified as an HGNC curie, return associated diseases. """
        ehgnc = urllib.parse.quote_plus(gene_node.identifier)
        logging.getLogger('application').debug('          biolink: %s/bioentity/gene/%s/diseases' % (self.url, ehgnc))
        r = requests.get('%s/bioentity/gene/%s/diseases' % (self.url, ehgnc)).json()
        edge_nodes = [ ]
        for association in r['associations']:
            if 'publications' in association and association['publications'] is not None:
                pubs = [ {'id': pub['id']} for pub in association['publications'] ]
            else:
                pubs = []
            obj = KNode(association['object']['id'], node_types.DISEASE, association['object']['label'] )
            rel = { 'typeid': association['relation']['id'], 'label':association['relation']['label'] }
            props = { 'publications': pubs, 'relation':rel }
            edge = KEdge( 'biolink', 'gene_get_disease', props )
            edge_nodes.append( (edge , obj ) )
        return edge_nodes
    def get_gene_function (self, gene):
        url = "{0}/bioentity/gene/{1}/function/".format (self.url, gene.identifier.strip ())
        response = requests.get (url).json ()
        return [
            (
                self.get_edge (response, 'molecular_function'),
                KNode(obj.replace ('GO:','GO.MOLECULAR_FUNCTION:'), node_types.FUNCTION)
            ) for obj in response['objects']
        ]

    def gene_get_genetic_condition(self, gene):
        """Given a gene specified as an HGNC curie, return associated genetic conditions.
        A genetic condition is specified as a disease that descends from a ndoe for genetic disease in MONDO."""
        disease_relations = self.gene_get_disease(gene)
        #checker = Mondo(ServiceContext.create_context ())
        relations = []
        for relation, obj in disease_relations:
            is_genetic_condition, new_object_ids = self.checker.is_genetic_disease(obj)
            if is_genetic_condition:
                obj.properties['mondo_identifiers'] = new_object_ids
                obj.node_type = node_types.GENETIC_CONDITION
                relations.append( (relation,obj) )
        #print (" biolink relations %s" % relations)
        return relations

def test():
    """What do we get back for HBB"""
    relations = gene_get_disease(('HGNC:4827',))
    checker = Mondo(ServiceContext.create_context ())
    for p, a in relations:
        igc, nid = checker.is_genetic_disease(a)
        print(a['id'], igc, nid)

def test_output():
    ehgnc = urllib.parse.quote_plus("HGNC:6136")
    r = requests.get('https://api.monarchinitiative.org/api/bioentity/gene/%s/diseases' % ehgnc).json()
    import json
    with open('testbiolink.json','w') as outf:
        json.dump(r, outf, indent=4)


if __name__ == '__main__':
    #test_output()
    b = Biolink (ServiceContext.create_context ())
#    print (b.get_gene_function (KNode('UniProtKB:P10721', 'G')))
    print (b.gene_get_genetic_condition (KNode ('DOID:2841', node_types.DISEASE)))
