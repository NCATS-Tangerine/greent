import logging
import json
import graphene
import pprint
import unittest
from exposures import Exposures
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

class TestGreenT(unittest.TestCase):

    greenT = GreenT ()

    def test_exposures (self):
        exposure_type = "pm25"
        start_date = "2010-1-1",
        end_date = "2010-1-7",
        lat = "35.9131996"
        lon = "-79.0558445"
        exposure_point = ",".join ([ lat, lon ]) #"35.9131996,-79.0558445"
        
        results = self.greenT.get_exposure_scores (exposure_type, start_date, end_date, exposure_point)
        for r in results.data ['exposureScores']:
            this_lat = str(r['latitude'])
            #print ("-------- ({}) ({})".format (this_lat, lat))
            self.assertTrue (this_lat == lat)

        #self.greenT.print_exposure_results (results, key='exposureScores')
        
        results = self.greenT.get_exposure_values (exposure_type, start_date, end_date, exposure_point)
        for r in results.data ['exposureValues']:
            this_lat = str(r['latitude'])
            #print ("-------- ({}) ({})".format (this_lat, lat))
            self.assertTrue (this_lat == lat)

        #self.greenT.print_exposure_results (results, key='exposureValues')

    def test_chembio (self):
        chemicals = [ 'D052638' ]
        conditions = self.greenT.get_exposure_conditions (chemicals)
        t = False
        for c in conditions:
            if c["gene"] == "http://chem2bio2rdf.org/uniprot/resource/gene/IL6":
                t = True
                break
        self.assertTrue (t)
        # print (json.dumps (conditions, indent=2))
        drugs = self.greenT.get_drugs_by_condition (conditions=[ "d001249" ])
        # print (json.dumps (drugs, indent=2))
        for d in [ "Paricalcitol", "NIMESULIDE", "Ramipril" ]:
            self.assertTrue (d in drugs)

        #paths = self.greenT.get_genes_pathways_by_disease (diseases)
        #print (paths)

if __name__ == '__main__':
    unittest.main()

