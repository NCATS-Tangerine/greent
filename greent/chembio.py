from string import Template
import os
import logging
from greent.triplestore import TripleStore

class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)
logger = LoggingUtil.init_logging (__file__)

class ChemBioKS(object):
    """ Generic service endpoints for medical and bio-chemical data. This set
        comprises portions of chem2bio2rdf, Monarch, and CTD environmental
        exposures."""
    def __init__(self, triplestore):
        self.triplestore = triplestore
    def get_template (self, query_name):
        query = None
        fn = os.path.join(os.path.dirname(__file__), 'query',
            '{0}.sparql'.format (query_name))
        with open (fn, 'r') as stream:
            text = stream.read ()
            query = Template (text)
            logger.debug ('query template: {0}', query)
        return query
    #@provenance()
    def query_chembio (self, query):
        """ Execute and return the result of a SPARQL query. """
        return self.triplestore.execute_query (query)

    def get_exposure_conditions (self, chemicals):
        """ Identify conditions (MeSH IDs) triggered by the specified stressor
            agent ids (also MeSH IDs).

        :param chemicals: List of IDs for substances of interest.
        :type chemicals: list of MeSH IDs, eg. D052638
        """
        id_list = ' '.join (list(map (lambda d : "( mesh:{0} )".format (d),
                            chemicals)))
        text = self.get_template ("ctd_gene_expo_disease").\
            safe_substitute (chemicals=id_list)
        results = self.triplestore.execute_query (text)
        return list(map (lambda b : {
            "chemical" : b['chemical'].value,
            "gene"     : b['gene'].value,
            "pathway"  : b['kegg_pathway'].value,
            "pathName" : b['pathway_name'].value,
            "pathID"   : b['pathway_id'].value,
            "human"    : '(human)' in b['pathway_name'].value
        },
                         results.bindings))

    def get_drugs_by_condition (self, conditions):
        """ Get drugs associated with a set of conditions.

        :param conditions: Conditions to find associated drugs for.
        :type conditions: List of MeSH IDs for conditions, eg.: D001249
        """
        condition_list = ' '.join (list(map (lambda d : "( mesh:{0} )".format (d), conditions)))
        text = self.get_template ("get_drugs_by_disease").substitute (conditions=condition_list)
        results = self.triplestore.execute_query (text)
        return list(map (lambda b : b['generic_name'].value, results.bindings))

    def get_genes_pathways_by_disease (self, diseases):
        """ Get genes and pathways associated with specified conditions.

        :param diseases: List of conditions designated by MeSH ID.
        :return: Returns a list of dicts containing gene and path information.
        """
        diseaseMeshIDList = ' '.join (list(map (lambda d : "( mesh:{0} )".format (d), diseases)))
        text = self.get_template ("genes_pathways_by_disease").safe_substitute (diseaseMeshIDList=diseaseMeshIDList)
        results = self.triplestore.execute_query (text)
        return list(map (lambda b : {
            "uniprotGene" : b['uniprotGeneID'].value,
            "keggPath"    : b['keggPath'].value,
            "pathName"    : b['pathwayName'].value,
            "human  "     : '(human)' in b['pathwayName'].value
        },
        results.bindings))
