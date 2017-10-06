import datetime
import logging
import json
import pprint
import unittest
import os
from collections import defaultdict
from greent.triplestore import TripleStore
from greent.chembio import ChemBioKS
from greent.chemotext import Chemotext
#from greent.exposures import Exposures
from greent.clinical import Clinical
from greent.disease_ont import DiseaseOntology
from greent.cmaq import CMAQ
from greent.pharos import Pharos
from greent.oxo import OXO
from greent.translator import Translator

class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)

logger = LoggingUtil.init_logging (__file__)

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
        self.pharos = Pharos ()
        self.oxo = OXO ()
        #self.init_translator ()
        self.translator = Translator (core=self)
        
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
    
    def init_translator (self):
        root_kind         = 'http://identifiers.org/doi'

        # MESH
        mesh              = 'http://identifiers.org/mesh'
        mesh_disease_name = 'http://identifiers.org/mesh/disease/name'
        mesh_drug_name    = 'http://identifiers.org/mesh/drug/name'
        mesh_disease_id   = 'http://identifiers.org/mesh/disease/id'
        
        # Disease
        doid_curie          = "doid"
        doid                = "http://identifiers.org/doid"
        pharos_disease_name = "http://pharos.nih.gov/identifier/disease/name"
        
        # DRUG
        c2b2r_drug_name   = "http://chem2bio2rdf.org/drugbank/resource/Generic_Name"

        # TARGET
        c2b2r_gene        = "http://chem2bio2rdf.org/uniprot/resource/gene"

        # PATHWAY
        c2b2r_pathway     = "http://chem2bio2rdf.org/kegg/resource/kegg_pathway"
        
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
        self.translator_router[doid][mesh_disease_id]             = lambda doid:    self.disease_ontology.doid_to_mesh (doid.upper())
        self.translator_router[c2b2r_drug_name][c2b2r_gene]       = lambda drug:    self.chembio_ks.drug_name_to_gene_symbol (drug)
        self.translator_router[c2b2r_gene][c2b2r_pathway]         = lambda gene:    self.chembio_ks.gene_symbol_to_pathway (gene)
        self.translator_router[c2b2r_gene][pharos_disease_name]   = lambda gene:    self.pharos.target_to_disease (gene)
        self.translator_router[mesh][root_kind]                   = lambda mesh_id: self.oxo.mesh_to_other (mesh_id)
        
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

    
