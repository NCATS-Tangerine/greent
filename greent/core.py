import datetime
import json
import logging
import os
import pprint
import unittest
from collections import defaultdict
from greent.chembio import ChemBioKS
from greent.chemotext import Chemotext
from greent.clinical import Clinical
from greent.cmaq import CMAQ
from greent.disease_ont import DiseaseOntology
from greent.endotype import Endotype
from greent.hetio import HetIO
from greent.oxo import OXO
from greent.pharos import Pharos
from greent.translator import Translator
from greent.triplestore import TripleStore
from greent.util import Config
from greent.util import LoggingUtil
from pprint import pprint

logger = LoggingUtil.init_logging (__file__)

class GreenT:

    ''' The Green Translator API - a single Python interface aggregating access mechanisms for 
    all Green Translator services. '''

    def __init__(self, config="greent.conf"):
        self.config = Config (config)

        self.blazegraph = TripleStore (self.get_url("chembio"))
        self.chembio_ks = ChemBioKS (self.blazegraph)

        self.clinical = Clinical (swagger_endpoint_url=self.get_url ("clinical"))
        self.exposures = CMAQ (self.get_url("cmaq"))
        self.chemotext = Chemotext (self.get_url("chemotext"))
        self.disease_ontology = DiseaseOntology (obo_resource=self.get_url("diseaseontology"))
        self.pharos = Pharos (self.get_url("pharos"))
        self.oxo = OXO (self.get_url("oxo"))
        self.hetio = HetIO (self.get_url("hetio"))
        self.endotype = Endotype (self.get_url("endotype"))
        self.translator = Translator (core=self)
    def get_url (self, svc):
        return self.config.get_service (svc)["url"]
    def get_config (self, key, default):
        result = None
        if key in self.config:
            result = self.config[key]
        if not result:
            result = default
        return result
                                  
    # Exposure API

    def get_exposure_scores (self, exposure_type, start_date, end_date, exposure_point):
        return self.exposures.get_scores (
            exposure_type = exposure_type,
            start_date    = start_date,
            end_date      = end_date,
            lat_lon       = exposure_point)

    def get_exposure_values (self, exposure_type, start_date, end_date, exposure_point):
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

    def execute (self, request):
        return self.translator.translate_chain (request)

if __name__ == "__main__":    
    g = GreenT ()
    response = g.translator.translate_chain (request={
        "iri"     : "mox://drug/gene/pathway/cell/anatomy/disease",
        "drug"    : "Aspirin",
        "disease" : "Asthma"
    })
    pprint (response)


# http://purl.obolibrary.org/obo/mondo.obo

