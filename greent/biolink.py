import requests
import urllib
from greent.mondo import Mondo
from reasoner.graph_components import KNode,KEdge
import logging

class Biolink:
    def __init__(self, url="https://api.monarchinitiative.org/api"):
        self.url = url
    def gene_get_disease(self, gene_node):
        """Given a gene specified as an HGNC curie, return associated diseases. """
        #TODO: we're assuming that gene_node.identifier is a valid curie for calling inti biolink - validate
        ehgnc = urllib.parse.quote_plus(gene_node.identifier)
        logging.getLogger('application').debug('          biolink: %s/bioentity/gene/%s/diseases' % (self.url, ehgnc))
        r = requests.get('%s/bioentity/gene/%s/diseases' % (self.url, ehgnc)).json()
        edge_nodes = [ ]
        #TODO:  Do I just want to suck in everything?  It's probably smarter, but for now it's mostly nulls
        #       and there's some stuff I'm completely unclear on (evidence graph).  In the long run, though,
        #       probably yes.
        for association in r['associations']:
            if 'publications' in association and association['publications'] is not None:
                pubs = [ {'id': pub['id']} for pub in association['publications'] ]
            else:
                pubs = []
            obj = KNode(association['object']['id'], 'D', association['object']['label'] )
            rel = { 'typeid': association['relation']['id'], 'label':association['relation']['label'] }
            props = { 'publications': pubs, 'relation':rel }
            edge = KEdge( 'biolink', 'queried', props )
            edge_nodes.append( (edge , obj ) )
        #TODO: WARN if no edges found?
        #TODO: DEBUG number of edges found / query.id
        #print (edge_nodes)
        return edge_nodes
    def gene_get_genetic_condition(self, gene):
        """Given a gene specified as an HGNC curie, return associated genetic conditions.
        
        A genetic condition is specified as a disease that descends from a ndoe for genetic disease in MONDO."""

        disease_relations = self.gene_get_disease(gene)
        checker = Mondo()
        relations = []
        for relation, obj in disease_relations:
            is_genetic_condition, new_object_ids = checker.is_genetic_disease(obj)
            if is_genetic_condition:
                obj.properties['mondo_identifiers'] = new_object_ids
                relations.append( (relation,obj) )
        print (" biolink relations %s" % relations)
        return relations

def test():
    """What do we get back for HBB"""
    print('hi')
    relations = gene_get_disease(('HGNC:4827',))
    checker = Mondo()
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
    test_output()
