import requests
import urllib
from greent.service import Service
from greent.service import ServiceContext
from greent.ontologies.mondo import Mondo
from greent.ontologies.go import GO
from greent.util import Text
from greent.graph_components import KNode, KEdge
from greent import node_types
import logging


class Biolink(Service):
    """ Preliminary interface to Biolink. Will move to automated Translator Registry invocation over time. """

    def __init__(self, context):
        super(Biolink, self).__init__("biolink", context)
        # TODO, can we just use the Mondo that's inthe core already?
        self.checker = Mondo(ServiceContext.create_context())
        self.go = GO(ServiceContext.create_context())

    def process_associations(self, r, predicate, target_node_type, reverse=False):
        """Given a response from biolink, create our edge and node structures.
        Sometimes (as in pathway->Genes) biolink returns the query as the object, rather
        than the subject.  reverse=True will handle this case, bringing back the subject
        of the response, rather than the object."""
        edge_nodes = []
        for association in r['associations']:
            pubs = []
            if 'publications' in association and association['publications'] is not None:
                for pub in association['publications']:
                    # Sometimes, we get back something like "uniprotkb" instead of a PMID.  We don't want it.
                    pubid_prefix = pub['id'][:4].upper()
                    if pubid_prefix == 'PMID':
                        pubs.append(pub['id'])
            if reverse:
                obj = KNode(association['subject']['id'], target_node_type, association['subject']['label'])
            else:
                obj = KNode(association['object']['id'], target_node_type, association['object']['label'])
            rel = {'typeid': association['relation']['id'], 'label': association['relation']['label']}
            props = {'publications': pubs, 'relation': rel}
            edge = KEdge('biolink', predicate, props)
            edge_nodes.append((edge, obj))
        return edge_nodes

    def gene_get_disease(self, gene_node):
        """Given a gene specified as an HGNC curie, return associated diseases. """
        ehgnc = urllib.parse.quote_plus(gene_node.identifier)
        logging.getLogger('application').debug('          biolink: %s/bioentity/gene/%s/diseases' % (self.url, ehgnc))
        r = requests.get('%s/bioentity/gene/%s/diseases' % (self.url, ehgnc)).json()
        return self.process_associations(r, 'gene_get_disease', node_types.DISEASE)

    def disease_get_phenotype(self, disease):
        url = "{0}/bioentity/disease/{1}/phenotypes/".format(self.url, disease.identifier)
        response = requests.get(url).json()
        return self.process_associations(response, 'disease_get_phenotype', node_types.PHENOTYPE)

    def gene_get_go(self, gene):
        # this function is very finicky.  gene must be in uniprotkb, and the curie prefix must be correctly capitalized
        url = "{0}/bioentity/gene/UniProtKB:{1}/function/".format(self.url, Text.un_curie(gene.identifier))
        response = requests.get(url).json()
        # return [ (a['object']['id'] , a['object']['label']) for a in response['associations'] ]
        return self.process_associations(response, 'gene_get_go', node_types.PROCESS)

    def gene_get_function(self, gene):
        edges_nodes = self.gene_get_go(gene)
        process_results = list(filter(lambda x: self.go.is_molecular_function(x[1].identifier), edges_nodes))
        for edge, node in process_results:
            edge.predicate = 'gene_get_molecular_function'
            node.identifier.replace('GO:', 'GO.MOLECULAR_FUNCTION:')
            node.node_type = node_types.FUNCTION
        return process_results

    def gene_get_process(self, gene):
        edges_nodes = self.gene_get_go(gene)
        process_results = list(filter(lambda x: self.go.is_biological_process(x[1].identifier), edges_nodes))
        for edge, node in process_results:
            edge.predicate = 'gene_get_biological_process'
            node.identifier.replace('GO:', 'GO.BIOLOGICAL_PROCESS:')
            node.node_type = node_types.PROCESS
        return process_results

    def gene_get_pathways(self, gene):
        url = "{0}/bioentity/gene/{1}/pathways/".format(self.url, gene.identifier)
        response = requests.get(url).json()
        return self.process_associations(response, 'gene_get_pathways', node_types.PATHWAY)

    def gene_get_react_pathway(self, gene):
        process_results = self.gene_get_pathways(gene)
        return list(filter(lambda en: en[1].identifier.startswith('REACT:'), process_results))

    def gene_get_kegg_pathway(self, gene):
        process_results = self.gene_get_pathways(gene)
        return list(filter(lambda en: en[1].identifier.startswith('KEGG-path:'), process_results))

    def pathway_get_gene(self, pathway):
        url = "{0}/bioentity/pathway/{1}/genes/".format(self.url, pathway.identifier)
        response = requests.get(url).json()
        return self.process_associations(response, 'pathway_get_genes', node_types.GENE, reverse=True)

    def gene_get_genetic_condition(self, gene):
        """Given a gene specified as an HGNC curie, return associated genetic conditions.
        A genetic condition is specified as a disease that descends from a ndoe for genetic disease in MONDO."""
        disease_relations = self.gene_get_disease(gene)
        # checker = Mondo(ServiceContext.create_context ())
        relations = []
        for relation, obj in disease_relations:
            is_genetic_condition, new_object_ids = self.checker.is_genetic_disease(obj)
            if is_genetic_condition:
                obj.properties['mondo_identifiers'] = new_object_ids
                obj.node_type = node_types.GENETIC_CONDITION
                relations.append((relation, obj))
        # print (" biolink relations %s" % relations)
        return relations


def test():
    """What do we get back for HBB"""
    b = Biolink(ServiceContext.create_context())
    relations = b.gene_get_disease(('HGNC:4827',))
    checker = Mondo(ServiceContext.create_context())
    for p, a in relations:
        igc, nid = checker.is_genetic_disease(a)
        print(a['id'], igc, nid)


def test_output():
    ehgnc = urllib.parse.quote_plus("HGNC:6136")
    r = requests.get('https://api.monarchinitiative.org/api/bioentity/gene/%s/diseases' % ehgnc).json()
    import json
    with open('testbiolink.json', 'w') as outf:
        json.dump(r, outf, indent=4)


def test_go():
    KIT_protein = KNode('UniProtKB:P10721', node_types.GENE)
    b = Biolink(ServiceContext.create_context())
    results = b.gene_get_process(KIT_protein)
    for ke, kn in results:
        print(ke, kn)


def test_phenotypes():
    asthma = KNode('DOID:2841', node_types.DISEASE)
    b = Biolink(ServiceContext.create_context())
    results = b.disease_get_phenotype(asthma)
    for ke, kn in results:
        print(ke, kn)


def test_gc():
    b = Biolink(ServiceContext.create_context())
    gene = KNode('HGNC:4851', node_type=node_types.GENE)
    results = b.gene_get_disease(gene)
    for e, k in results:
        print(k)
    results = b.gene_get_genetic_condition(gene)
    print(results)


def test_pathways():
    b = Biolink(ServiceContext.create_context())
    gene = KNode('HGNC:5013', node_type=node_types.GENE)
    results = b.gene_get_kegg_pathway(gene)
    for e, k in results:
        print(k)
    results = b.gene_get_react_pathway(gene)
    for e, k in results:
        print(k)
    k = results[0][1]
    results = b.pathway_get_gene(k)
    for e, k in results:
        print(k)


if __name__ == '__main__':
    test_pathways()
    # test_output()
    # b = Biolink (ServiceContext.create_context ())
#    print (b.get_gene_function (KNode('UniProtKB:P10721', 'G')))
# print (b.gene_get_genetic_condition (KNode ('DOID:2841', node_types.DISEASE)))
