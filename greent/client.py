import json
import logging
import unittest
import requests
import sys
import traceback
import requests

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        level = logging.DEBUG
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=level)
        return logging.getLogger(name)

logger = LoggingUtil.init_logging (__file__)

class GraphQL (object):

    ''' GraphQL client for the Green Translator. '''

    def __init__(self, url="http://localhost:5000/graphql"):
        self.url = url

    def query (self, query):
        logger.debug ("query: %s", query)
        r = requests.post (self.url,
                           data = json.dumps (query).encode ('utf8'),
                           headers = { 'Content-Type': 'application/json' })
        r.raise_for_status ()
        return r.json ()

    # Exposures

    def get_exposure_scores (self, exposure_type, start_date, end_date, exposure_point):
        return self.query ({
            "query" : """query getExposures ($type : String, $start : String, $end : String, $point : String) {
                           exposureScore(type: $type, startDate: $start, endDate: $end, exposurePoint: $point)
                       }""",
            "variables" : {
                "type"  : exposure_type,
                "start" : start_date,
                "end"   : end_date,
                "point" : exposure_point
            }
        })

    def get_exposure_values (self, exposure_type, start_date, end_date, exposure_point):
        return self.query ({
            "query" : """query getValues ($type : String, $start : String, $end : String, $point : String) {
                           exposureValue(type: $type, startDate: $start, endDate: $end, exposurePoint: $point)
                       }""",
            "variables" : {
                "type"  : exposure_type,
                "start" : start_date,
                "end"   : end_date,
                "point" : exposure_point
            }
        })

    # ChemBio API

    def get_exposure_conditions (self, chemicals):
        return self.query ({
            "query" : """query getExposureConditions($chem : String ) {
                             exposureConditions(chemicals: $chem)
                      }""",
            "variables" : {
                "chem" : json.dumps (chemicals)
            }
        })

    def get_drugs_by_condition (self, conditions):
        return json.dumps (self.query ({
            "query" : """query getDrugsByCondition($cond : String ) {
                          drugsByCondition(conditions:$cond)
                       }""",
            "variables" : {
                "cond" : json.dumps (conditions)
            }
        }))

    def get_genes_pathways_by_disease (self, diseases):
        return json.dumps (self.query ({
            "query" : """query getGenesPathways($diseases : String ) {
                            genesPathwaysByDisease(diseases:$diseases)
                         }""",
            "variables" : {
                "diseases" : json.dumps (diseases)
            }
        }))


    # Clinical API

    def get_patients (self, age=None, sex=None, race=None, location=None):
        return self.query ({
            "query" : """
                query queryPatients ($age : String, $sex : String, $race : String) {
                    patients (age: $age, sex: $sex, race: $race)
                }
            """,
            "variables" : {
                "age"  : "8",
                "sex"  : "male",
                "race" : "black"
            }
        })

class TestGraphQLClient (unittest.TestCase):
 
    client = GraphQL ()

    def test_get_exposure_scores (self):
        lat = "35.9131996"
        lon = "-79.0558445"
        response = self.client.get_exposure_scores (
            exposure_type = "pm25",
            start_date = "2010-01-01",
            end_date = "2010-01-07",
            exposure_point = ",".join ([ lat, lon ]))
        #print ("-----------> {}".format (response))
        text = response['data']['exposureScore']
        result = json.loads (text)
        #print (json.dumps (result, indent=2))
        self.assertTrue (type (result[0]['latitude']) == float)
        self.assertTrue (type (result[0]['longitude']) == float)
        print ("Verified exposure response structure")

    def test_get_exposure_values (self):
        lat = "35.9131996"
        lon = "-79.0558445"
        response = self.client.get_exposure_values (
            exposure_type = "pm25",
            start_date = "2010-01-01",
            end_date = "2010-01-07",
            exposure_point = ",".join ([ lat, lon ]))
        text = response['data']['exposureValue']
        result = json.loads (text)
        #print (json.dumps (result, indent=2))
        self.assertTrue (type (result[0]['latitude']) == float)
        self.assertTrue (type (result[0]['longitude']) == float)
        print ("Verified exposure response structure")

    def test_get_exposure_conditions (self):
        result = self.client.get_exposure_conditions ([ "D052638" ])
        result = json.loads (result ['data']['exposureConditions'])
        result = sorted (result, key=lambda v : v['gene'])
        self.assertTrue (result[0]['gene'] == 'http://chem2bio2rdf.org/uniprot/resource/gene/COL1A1')

    def test_get_drugs_by_condition (self):
        result = json.loads (self.client.get_drugs_by_condition ([ "d001249" ]))
        result = json.loads (result['data']['drugsByCondition'])
        self.assertTrue ("Melphalan" in result)

    def test_get_genes_pathways_by_disease (self):
        result = json.loads (self.client.get_genes_pathways_by_disease ([ "d001249" ]))
        result = json.loads (result['data']['genesPathwaysByDisease'])
        result = sorted (result, key=lambda v : v['uniprotGene'])
        self.assertTrue (result[0]['uniprotGene'] == 'http://chem2bio2rdf.org/uniprot/resource/gene/ABCB1')

#    def test_clinical (self):
#        print (self.client.get_patients ())
    
if __name__ == '__main__':
    unittest.main()

