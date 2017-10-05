import datetime
import logging
import json
import pprint
import unittest
import os
from greent.triplestore import TripleStore
from greent.chembio import ChemBioKS
from greent.exposures import Exposures
from greent.clinical import Clinical
from greent.chemotext import Chemotext
from greent.disease_ont import DiseaseOntology
from greent.cmaq import CMAQ
from collections import defaultdict

class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)

logger = LoggingUtil.init_logging (__file__)

class NoTranslation (Exception):
    def __init__(self, message=None):
        super(NoTranslation, self).__init__(message)

class GreenT (object):

    ''' The Green Translator API - a single Python interface aggregating access mechanisms for 
    all Green Translator services. '''

    def __init__(self, config={}):
        self.config = config
        
        blaze_uri = self.get_config ('blaze_uri', 'http://stars-blazegraph.renci.org/bigdata/sparql')
        self.blazegraph = TripleStore (blaze_uri)

        self.chembio_ks = ChemBioKS (self.blazegraph)
        clinical_url = self.get_config ('clinical_url', "http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE")

        self.clinical = Clinical (swagger_endpoint_url=clinical_url)
        exposures_uri = self.get_config ("exposures_uri",
                                         "https://app.swaggerhub.com/apiproxy/schema/file/mjstealey/environmental_exposures_api/0.0.1/swagger.json")
        self.exposures = CMAQ (exposures_uri)
        self.chemotext = Chemotext ()
        self.disease_ontology = DiseaseOntology ()
        self.init_translator ()
        
    def get_config (self, key, default):
        result = None
        if key in self.config:
            result = self.config[key]
        if not result:
            result = default
        return result
                                  
    # Exposure API

    def get_exposure_scores (self, exposure_type, start_date, end_date, exposure_point):
        #print ("core -------------> {}".format (start_date))
        start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        return self.exposures.get_scores (
            exposure_type = exposure_type,
            start_date    = start_date,
            end_date      = end_date,
            lat_lon       = exposure_point)

    def get_exposure_values (self, exposure_type, start_date, end_date, exposure_point):
        start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        print (" start_date_obj: ({})".format (start_date_obj))
        print (" end_date_obj: ({})".format (end_date_obj))
        print (" exposure_type: ({})".format (exposure_type))
        print (" exposure_point: ({})".format (exposure_point))
        return self.exposures.get_values (
            exposure_type  = exposure_type,
            start_date     = start_date,
            end_date       = end_date,
            lat_lon        = exposure_point)

    # ChemBio API

    def get_exposure_conditions_json (self, chemicals):
        return json.dumps (self.get_exposure_conditions (chemicals))

    def get_exposure_conditions (self, chemicals):
        return self.chembio_ks.get_exposure_conditions (chemicals)

    def get_drugs_by_condition_json (self, conditions):
        return json.dumps (self.get_drugs_by_condition (conditions))

    def get_drugs_by_condition (self, conditions):
        return self.chembio_ks.get_drugs_by_condition (conditions)

    def get_genes_pathways_by_disease_json (self, diseases):
        return json.dumps (self.get_genes_pathways_by_disease (diseases))

    def get_genes_pathways_by_disease (self, diseases):
        return self.chembio_ks.get_genes_pathways_by_disease (diseases)

    def get_drug_gene_disease (self, disease_name, drug_name):
        return self.chembio_ks.get_drug_gene_disease (disease_name, drug_name)
    
    # Clinical API

    def get_patients (self, age=None, sex=None, race=None, location=None):
        return self.clinical.get_patients (age, sex, race, location)

    # Translator

    def drug_name_to_gene_symbol (self, drug_name):
        return self.chembio_ks.query (
            input_fields = { "drugName" : drug_name },
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
    
    def init_translator (self):
        root_kind         = 'http://identifiers.org/doi/'

        # MESH
        mesh_disease_name = 'http://identifiers.org/mesh/disease/name/'
        mesh_drug_name    = 'http://identifiers.org/mesh/drug/name/'
        mesh_disease_id   = 'http://identifiers.org/mesh/disease/id'
        
        # DOID
        doid_curie        = "doid"
        doid              = "http://identifiers.org/doid/"

        # DRUG
        c2b2r_gene        = "http://chem2bio2rdf.org/uniprot/resource/gene"
        c2b2r_drug_name   = "http://chem2bio2rdf.org/drugbank/resource/Generic_Name"

        # Semantic equivalence
        
        self.equivalence = defaultdict(lambda: [])
        self.equivalence[doid_curie] = [ doid ]

        # https://github.com/prefixcommons/biocontext/blob/master/registry/uber_context.jsonld
        uber_context_path = os.path.join(os.path.dirname(__file__), 'jsonld', 'uber_context.jsonld')
        with open (uber_context_path, 'r') as stream:
            self.equivalence = json.loads (stream.read ())["@context"]
            self.equivalence["MESH"] = self.equivalence ["MESH.2013"]
            
        # Domain translation
        self.translator_router = defaultdict (lambda: defaultdict (lambda: NoTranslation ()))
        self.translator_router[mesh_disease_name][mesh_drug_name] = lambda disease: self.chemotext.disease_name_to_drug_name (disease)
        self.translator_router[doid][mesh_disease_id] = lambda doid: self.disease_ontology.doid_to_mesh (doid.upper())
        self.translator_router[c2b2r_drug_name][c2b2r_gene] = lambda drug_name: self.drug_name_to_gene_symbol (drug_name)
        
    def resolve_id (self, an_id, domain):
        if not an_id in domain:
            candidate = an_id
            an_id = None
            for alternative in self.equivalence[candidate]:
                if alternative in domain:
                    # Postpone the problem of synonymy
                    an_id = alternative
                    logger.debug ("Selected alternative id {0} for input {1}".format (an_id, candidate))
                    break
                # Also, if all candidates turn out not to be in the domain, we could recurse to try synonyms for them
        return an_id
    
    def translate (self, thing, domainA, domainB):
        result = None
        resolvedA = self.resolve_id (domainA, self.translator_router)
        resolvedB = self.resolve_id (domainB, self.translator_router[domainA])
        if resolvedA and resolvedB:
            result = self.translator_router[resolvedA][resolvedB] (thing)
            if isinstance (result, NoTranslation):
                raise NoTranslation ("No translation implemented from domain {0} to domain {1}".format (domainA, domainB))
        return result

    
