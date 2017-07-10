import logging
import json
import graphene
import pprint
import unittest
from exposures import Exposures
from clinical import Clinical
from SPARQLWrapper import SPARQLWrapper2, JSON
from string import Template
from triplestore import TripleStore
from chembio import ChemBioKS

class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)
logger = LoggingUtil.init_logging (__file__)

''' Use GraphQL to query the RENCI Environmental Exposures API. '''

class ExposureScore(graphene.ObjectType):
    ''' An exposure score. '''
    exposure_type = graphene.String ()
    start_time    = graphene.String ()
    end_time      = graphene.String ()
    latitude      = graphene.Float ()
    longitude     = graphene.Float ()
    units         = graphene.String ()
    value         = graphene.String ()

class ExposureValue(graphene.ObjectType):
    ''' An exposure value. '''
    exposure_type = graphene.String ()
    start_time    = graphene.String ()
    end_time      = graphene.String ()
    latitude      = graphene.Float ()
    longitude     = graphene.Float ()
    units         = graphene.String ()
    value         = graphene.String ()

class QueryExposureScores (graphene.ObjectType):
    exposure_scores = graphene.List (of_type=ExposureScore,
                                     description="Exposure scores")
    def resolve_exposure_scores (self, args, context, info):
        exposures = Exposures ()
        return exposures.get_scores (
            exposure_type  = args.get ("exposureType"),
            start_date     = args.get ("startDate"),
            end_date       = args.get ("endDate"),
            exposure_point = args.get ("exposurePoint"))

class QueryExposureValues (graphene.ObjectType):
    exposure_values = graphene.List (of_type=ExposureValue,
                                     description="Exposure values")
    def resolve_exposure_values (self, args, context, info):
        exposures = Exposures ()
        return exposures.get_values (
            exposure_type  = args.get ("exposureType"),
            start_date     = args.get ("startDate"),
            end_date       = args.get ("endDate"),
            exposure_point = args.get ("exposurePoint"))

class GreenT (object):

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

    def get_exposure_query (self, query_tag, exposure_type, start_date, end_date, exposure_point):
        query = Template ("""{
           ${query_tag} {
              value,
              units,
              latitude,
              longitude,
              exposureType,
              startTime,
              endTime
           }
        }""").substitute (query_tag=query_tag)

        context = {
            "exposureType"  : exposure_type,
            "startDate"     : start_date,
            "endDate"       : end_date,
            "exposurePoint" : exposure_point
        }
        return query, context
    
    def get_exposure_scores (self, exposure_type, start_date, end_date, exposure_point):
        query, context = self.get_exposure_query ("exposureScores", exposure_type, start_date, end_date, exposure_point)
        return graphene.Schema (query=QueryExposureScores).execute (query, context)

    def get_exposure_values (self, exposure_type, start_date, end_date, exposure_point):
        query, context = self.get_exposure_query ("exposureValues", exposure_type, start_date, end_date, exposure_point)
        return graphene.Schema (query=QueryExposureValues).execute (query, context)

    def print_exposure_results (self, result, key):
        if result.errors:
            print ("errors: {0}".format (result.errors))
        else:
            for result in result.data[key]:
                print( "  --score: type: {0} lat: {1} lon: {2} start: {3} end: {4} unit: {5} val: {6}".format (
                    result['exposureType'],
                    result['latitude'],
                    result['longitude'],
                    result['startTime'],
                    result['endTime'],
                    result['units'],
                    result['value']))

    def get_exposure_conditions (self, chemicals):
        return self.chembio_ks.get_exposure_conditions (chemicals)

    def get_drugs_by_condition (self, conditions):
        return self.chembio_ks.get_drugs_by_condition (conditions)

    def get_genes_pathways_by_disease (self, diseases):
        return self.chembio_ks.get_genes_pathways_by_disease (diseases)


    # Clinical:

    def get_patients (self, age=None, sex=None, race=None, location=None):
        return self.clinical.get_patients (age, sex, race, location)

