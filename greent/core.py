import datetime
import logging
import json
import pprint
import unittest
from greent.triplestore import TripleStore
from greent.chembio import ChemBioKS
from greent.exposures import Exposures
from greent.clinical import Clinical

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
        
        blaze_uri = None
        if 'blaze_uri' in config:
            blaze_uri = self.config ['blaze_uri']
        if not blaze_uri:
            blaze_uri = 'http://stars-blazegraph.renci.org/bigdata/sparql'
        self.blazegraph = TripleStore (blaze_uri)

        self.chembio_ks = ChemBioKS (self.blazegraph)
        self.clinical = Clinical ()
        self.exposures = Exposures ()

    # Exposure API

    def get_exposure_scores (self, exposure_type, start_date, end_date, exposure_point):
        #print ("core -------------> {}".format (start_date))
        start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        return self.exposures.get_scores (
            exposure_type = exposure_type,
            start_date = start_date_obj,
            end_date = end_date_obj,
            exposure_point = exposure_point)

    def get_exposure_values (self, exposure_type, start_date, end_date, exposure_point):
        start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        return self.exposures.get_values (
            exposure_type  = exposure_type,
            start_date     = start_date_obj,
            end_date       = end_date_obj,
            exposure_point = exposure_point)

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

    # Clinical API

    def get_patients (self, age=None, sex=None, race=None, location=None):
        return self.clinical.get_patients (age, sex, race, location)
