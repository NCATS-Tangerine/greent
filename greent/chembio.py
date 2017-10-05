from string import Template
import json
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
            "human"       : '(human)' in b['pathwayName'].value
        },
        results.bindings))

    
    def get_drug_gene_disease (self, disease_name, drug_name):
        """ Identify targets and diseases assocaited with a drug name.
        :param disease_name: MeSH name of a disease condition.
        :type str: String
        :param drug_name: Name of a drug.
        :type str: String
        """
        text = self.get_template ("drug_gene_disease").safe_substitute (
            diseaseName=disease_name,
            drugName=drug_name)
        results = self.triplestore.execute_query (text)
        return list(map (lambda b : {
            "uniprotSymbol" : b['uniprotSym'].value,
            "diseaseId"     : b['diseaseID'].value
        }, results.bindings))


    def drug_name_to_gene_symbol (self, drug_name):
        print ("-----------------------------------")
        result = self.query (
            input_fields = { "drugName" : drug_name },
            output_fields = [ 'uniprotSym' ],
            query_template="""
prefix db_resource:    <http://chem2bio2rdf.org/drugbank/resource/>
prefix ctd:            <http://chem2bio2rdf.org/ctd/resource/>
prefix pubchem:        <http://chem2bio2rdf.org/pubchem/resource/>
select ?uniprotSym where {
    values ( ?drugName ) { ( "$drugName" ) }
    ?ctdChemGene ctd:cid                        ?pubChemCID;
                 ctd:gene                       ?uniprotSym.
    ?ctdChemDis  ctd:cid                        ?pubChemCID;
                 ctd:diseasename                ?diseaseName.
    ?drugID      db_resource:CID                ?pubChemCID ;
  	         db_resource:Generic_Name       ?drugGenericName .
  filter regex(lcase(str(?drugGenericName)), lcase(?drugName))
}
LIMIT 200
            """)
        return result
    
    def gene_symbol_to_pathway (self, uniprot_symbol):
        uniprot_iri = "http://chem2bio2rdf.org/uniprot/resource/gene"
        if not uniprot_symbol.startswith (uniprot_iri):
            uniprot_symbol = "{0}/{1}".format (uniprot_iri, uniprot_symbol)
            print ("----------> {0}".format (uniprot_symbol))
        return self.query (
#    values ( ?ctdGene ) { ( <http://chem2bio2rdf.org/uniprot/resource/gene/$uniprotSymbol> ) }

            input_fields = { "uniprotSymbol" : uniprot_symbol },
            output_fields = [ "keggPath" ],
            query_template="""
prefix kegg:           <http://chem2bio2rdf.org/kegg/resource/> \n
prefix pharmgkb:       <http://chem2bio2rdf.org/pharmgkb/resource/> \n
prefix ctd:            <http://chem2bio2rdf.org/ctd/resource/> \n
select ?ctdGene ?uniprotID ?pathwayName ?keggPath where { \n
    values ( ?ctdGene ) { ( <$uniprotSymbol> ) } \n
    ?keggPath    kegg:protein    ?uniprotID ; kegg:Pathway_name ?pathwayName . \n
    ?pharmGene   pharmgkb:Symbol ?ctdGene ; pharmgkb:UniProt_Id ?uniprotID. \n
    ?ctdChemGene ctd:gene        ?ctdGene. \n
}
""")
    
    def query (self, query_template, output_fields, input_fields=[]):
        query_template = Template(query_template)
        query_text = query_template.safe_substitute (**input_fields)
        query_results = self.triplestore.execute_query (query_text)
        #print (query_results.bindings)
        result = set ()
        for b in query_results.bindings:
            for f in output_fields:
                result.add (b[f].value)
        return result
#        return { key : value.value for (key, value) in query_results.bindings }
        
