import json
import logging
import unittest
import requests
import sys
import traceback
import requests
from string import Template
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from greent.util import LoggingUtil
from greent.endotype import Endotype

logger = LoggingUtil.init_logging (__file__)

class GraphQL (object):

    ''' GraphQL client for the Green Translator. '''
    def __init__(self, url="https://stars-app.renci.org/greent/graphql"):
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
        response = self.query ({
            "query" : """
            query getExposureScore($exposureType:String, $startDate:String, $endDate:String, $exposurePoint:String){
              exposureScore(exposureType  : $exposureType, 
    	        startDate     : $startDate, 
    	        endDate       : $endDate, 
    	        exposurePoint : $exposurePoint) {
                  value
                  latitude
                  longitude
                }
              }""",
            "variables" : {
                "exposureType"  : exposure_type,
                "startDate"     : start_date,
                "endDate"       : end_date,
                "exposurePoint" : exposure_point
            }
        })
        return response['data']['exposureScore']['scores']

    def get_exposure_values (self, exposure_type, start_date, end_date, exposure_point):
        response = self.query ({
            "query" : """
            query getExposureValue($exposureType:String, $startDate:String, $endDate:String, $exposurePoint:String){
              exposureValue(exposureType  : $exposureType, 
    	        startDate     : $startDate, 
    	        endDate       : $endDate , 
    	        exposurePoint : $exposurePoint) {
                  value
                  latitude
                  longitude
                }
              }""",
            "variables" : {
                "exposureType"  : exposure_type,
                "startDate"     : start_date,
                "endDate"       : end_date,
                "exposurePoint" : exposure_point
            }
        })
        return response['data']['exposureValue']

    # ChemBio API

    def get_exposure_conditions (self, chemicals):
        response = self.query ({
            "query" : """
            query getExposureConditions($chem : [String] ) {
              exposureConditions(chemicals: $chem) {
                chemical
                gene
                pathway
                pathName
                pathID
                human
              }
            }""",
            "variables" : {
                "chem" : chemicals
            }
        })
        return response ['data']['exposureConditions']
    
    def get_drugs_by_condition (self, conditions):
        response = self.query ({
            "query" : """
            query getDrugsByCondition($cond : [String] ) {
              drugsByCondition(conditions:$cond) {
                genericName
              }
            }""",
            "variables" : {
                "cond" : conditions
            }
        })
        drugs = response['data']['drugsByCondition']
        return [ r['genericName'] for r in drugs ]
 
    def get_genes_pathways_by_disease (self, diseases):
        response = self.query ({
            "query" : """
            query getGenesPathways($diseases : [String] ) {
              genePathsByDisease(diseases:$diseases) {
                uniprotGene
                keggPath
                pathName
                human
              }
            }""",
            "variables" : {
                "diseases" : diseases
            }
        })
        return response['data']['genePathsByDisease']

    # Clinical API

    def get_patients (self, age=None, sex=None, race=None, location=None):
        response = self.query ({
            "query" : """
            query queryPatients ($age : Int, $sex : String, $race : String) {
              patients (age: $age, sex: $sex, race: $race) {
                birthDate
                race
                sex
                patientId
                diagnoses {
                  diagnosis
                }
                geoCode {
                  latitude
                  longitude
                }
                prescriptions {
                  medication
                  date
                }
              }
            }""",
            "variables" : {
                "age"  : age,
                "sex"  : sex,
                "race" : race
            }
        })
        return response['data']['patients']

    def get_endotypes (self, query):
        response = self.query ({
            "query" : """
               query get_endotype ( $query : String) {
                 endotype (query:$query)
               }""",
            "variables" : {
                "query" : query
            }
        })
        return response['data']
    
    def translate (self, thing, domain_a, domain_b):
        query_text = Template ("""{ translate (thing:\"$thing\", domainA: \"$domain_a\", domainB: \"$domain_b\") { value } }""").\
           safe_substitute (thing=thing, domain_a=domain_a, domain_b=domain_b)
        result = self.query ({
            "query" : query_text
        })['data']['translate']
        return result
    
class TestGraphQLClient (unittest.TestCase):
 
    client = GraphQL ("http://localhost:5000/graphql")

    def test_get_exposure_scores (self):
        lat = "35.9131996"
        lon = "-79.0558445"
        result = self.client.get_exposure_scores (
            exposure_type = "pm25",
            start_date = "2010-01-01",
            end_date = "2010-01-07",
            exposure_point = ",".join ([ lat, lon ]))
        self.assertTrue (result['latitude'] == None)
        self.assertTrue (result['longitude'] == None)
        print ("Verified exposure response structure")

    def test_get_exposure_values (self):
        lat = "35.9131996"
        lon = "-79.0558445"
        result = self.client.get_exposure_values (
            exposure_type = "pm25",
            start_date = "2010-01-01",
            end_date = "2010-01-07",
            exposure_point = ",".join ([ lat, lon ]))
        self.assertTrue (result['latitude'] == None)
        self.assertTrue (result['longitude'] == None)
        print ("Verified exposure response structure")

    def test_get_exposure_conditions (self):
        result = self.client.get_exposure_conditions ([ "D052638" ])
        result = sorted (result, key=lambda v : v['gene'])
        self.assertTrue (result[0]['gene'] == 'http://chem2bio2rdf.org/uniprot/resource/gene/COL1A1')

    def test_get_drugs_by_condition (self):
        result = self.client.get_drugs_by_condition ([ "d001249" ])
        self.assertTrue ("Melphalan" in result)

    def test_get_genes_pathways_by_disease (self):
        result = self.client.get_genes_pathways_by_disease ([ "d001249" ])
        result = sorted (result, key=lambda v : v['uniprotGene'])
        self.assertTrue (result[0]['uniprotGene'] == 'http://chem2bio2rdf.org/uniprot/resource/gene/ABCB1')

    def test_clinical (self):
        pass #print (self.client.get_patients ())

    def test_endotype (self):
        greent = GraphQL ("http://localhost:5000/graphql")
        exposures = list(map(lambda exp : Endotype.create_exposure (**exp), [{
            "exposure_type": "pm25",
            "units"        : "",
            "value"        : 2
        }]))
        visits = list(map(lambda v : Endotype.create_visit(**v), [{
            "icd_codes"  : "ICD9:V12,ICD9:E002",
            "lat"        : "20",
            "lon"        : "20",
            "time"       : "2017-10-12 21:12:29",
            "visit_type" : "INPATIENT",
            "exposures"  : exposures
        }]))
        request = Endotype.create_request (dob= "2017-10-04", model_type="M0", race="1", sex="M", visits = visits)
        greent.get_endotypes (query = json.dumps (request))
        
if __name__ == '__main__':
    unittest.main()

