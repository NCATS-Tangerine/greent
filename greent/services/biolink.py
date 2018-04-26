import requests
import urllib
from greent.service import Service
from greent.ontologies.mondo import Mondo
from greent.ontologies.go import GO
from greent.ontologies.mondo2 import Mondo2
from greent.ontologies.go2 import GO2
from greent.util import Text
from greent.graph_components import KNode, KEdge
from greent import node_types
from datetime import datetime as dt
import logging


class Biolink(Service):
    """ Preliminary interface to Biolink. Will move to automated Translator Registry invocation over time. """

    def __init__(self, context):
        super(Biolink, self).__init__("biolink", context)
        self.checker = context.core.mondo
        self.go = context.core.go

        
    def process_associations(self, r, function, target_node_type, input_identifier, url, reverse=False):
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
            predicate_id = association['relation']['id']
            predicate_label = association['relation']['label']
            if (predicate_id == None) or (':' not in predicate_id):
                predicate_id = f'biolink:{function}'
                predicate_label = f'biolink:{function}'
            standard_id, standard_label = self.standardize_predicate(predicate_id, predicate_label)
            edge = KEdge(f'biolink.{function}', dt.now(), predicate_id, predicate_label, input_identifier, standard_id, standard_label, publications = pubs, url = url)
            edge_nodes.append((edge, obj))
        return edge_nodes


    def gene_get_disease(self, gene_node):
        """Given a gene specified as a curie, return associated diseases."""
        #Biolink is pretty forgiving on gene inputs, and our genes should have HGNC as their identifiers nearly always
        ehgnc = urllib.parse.quote_plus(gene_node.identifier)
        logging.getLogger('application').debug('          biolink: %s/bioentity/gene/%s/diseases' % (self.url, ehgnc))
        urlcall = '%s/bioentity/gene/%s/diseases' % (self.url, ehgnc)
        r = requests.get(urlcall).json()
        return self.process_associations(r, 'gene_get_disease', node_types.DISEASE, ehgnc, urlcall)

    def disease_get_phenotype(self, disease):
        #Biolink should understand any of our disease inputs here.
        url = "{0}/bioentity/disease/{1}/phenotypes/".format(self.url, disease.identifier)
        response = requests.get(url).json()
        return self.process_associations(response, 'disease_get_phenotype', node_types.PHENOTYPE, disease.identifier, url)

    def gene_get_go(self, gene):
        # this function is very finicky.  gene must be in uniprotkb, and the curie prefix must be correctly capitalized
        uniprot_id = None
        for gene_synonym in gene.synonyms:
            curie = Text.get_curie(gene_synonym)
            if curie == 'UNIPROTKB':
                uniprot_id = gene_synonym
                break
        if uniprot_id is None:
            return []
        url = "{0}/bioentity/gene/UniProtKB:{1}/function/".format(self.url, Text.un_curie(uniprot_id))
        response = requests.get(url).json()
        return response,url,uniprot_id
        #return self.process_associations(response, 'gene_get_go', node_types.PROCESS, url)

    def gene_get_function(self, gene):
        response,url,input_id = self.gene_get_go(gene)
        edges_nodes = self.process_associations(response, 'gene_get_function', node_types.FUNCTION, input_id, url)
        function_results = list(filter(lambda x: self.go.is_molecular_function(x[1].identifier), edges_nodes))
        return function_results

    def gene_get_process(self, gene):
        response,url,input_id = self.gene_get_go(gene)
        edges_nodes = self.process_associations(response, 'gene_get_process', node_types.PROCESS, input_id, url)
        process_results = list(filter(lambda x: self.go.is_biological_process(x[1].identifier), edges_nodes))
        return process_results

    def gene_get_pathways(self, gene):
        url = "{0}/bioentity/gene/{1}/pathways/".format(self.url, gene.identifier)
        response = requests.get(url).json()
        return self.process_associations(response, 'gene_get_pathways', node_types.PATHWAY, gene.identifier, url)


    def pathway_get_gene(self, pathway):
        url = "{0}/bioentity/pathway/{1}/genes/".format(self.url, pathway.identifier)
        response = requests.get(url).json()
        return self.process_associations(response, 'pathway_get_genes', node_types.GENE, url, pathway.identifier, reverse=True)
