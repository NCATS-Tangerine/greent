import requests
import json
import urllib
from greent.service import Service
from greent.service import ServiceContext
from greent.mondo import Mondo
from greent.go import GO
from greent.util import Text
from reasoner.graph_components import KNode,KEdge
from reasoner import node_types
import logging

class Biolink(Service):
    """ Preliminary interface to Biolink. Will move to automated Translator Registry invocation over time. """
    def __init__(self, context):
        super(Biolink, self).__init__("biolink", context)
        self.checker = Mondo(ServiceContext.create_context ())
        self.go = GO(ServiceContext.create_context ())
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
    def gene_get_go(self,gene):
        #this function is very finicky.  gene must be in uniprotkb, and the curie prefix must be correctly capitalized
        url = "{0}/bioentity/gene/UniProtKB:{1}/function/".format (self.url, Text.un_curie(gene.identifier) )
        response = requests.get (url).json ()
        return [ (a['object']['id'] , a['object']['label']) for a in response['associations'] ]
    def gene_get_function (self, gene):
        response = self.gene_get_go( gene )
        return [
            (
                self.get_edge (props={}, predicate='molecular_function'),
                KNode(go_id.replace ('GO:','GO.MOLECULAR_FUNCTION:'), node_types.FUNCTION, label=go_label)
            ) for go_id, go_label in response if self.go.is_molecular_function(go_id)
        ]
    def gene_get_process ( self, gene):
        response = self.gene_get_go( gene )
        return [
            (
                self.get_edge (props={},  predicate='biological_process'),
                KNode(go_id.replace ('GO:','GO.BIOLOGICAL_PROCESS:'), node_types.PROCESS, label=go_label)
            ) for go_id, go_label in response if self.go.is_biological_process(go_id)
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

def test_go():
    KIT_protein = KNode('UniProtKB:P10721',node_types.GENE)
    b = Biolink (ServiceContext.create_context ())
    results=b.gene_get_process(KIT_protein)
    for ke, kn in results:
        print(ke, kn)

if __name__ == '__main__':
    test_go()
    #test_output()
    #b = Biolink (ServiceContext.create_context ())
#    print (b.get_gene_function (KNode('UniProtKB:P10721', 'G')))
    #print (b.gene_get_genetic_condition (KNode ('DOID:2841', node_types.DISEASE)))
