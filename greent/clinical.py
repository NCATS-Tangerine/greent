import inspect
import json
import requests
import traceback
import unittest
from datetime import date
from datetime import datetime, timedelta
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from string import Template
from uuid import getnode as get_mac

class SwaggerEndpoint(object):

    def __init__(self, swagger_endpoint_url):
        http_client = RequestsClient ()
        # TODO: fix ssl certificate.
#        http_client.session.verify = False
        self.client = SwaggerClient.from_url(
            swagger_endpoint_url,
            http_client=http_client)

    def inspect (self):
        for name, obj in inspect.getmembers (self.client):
            print ("-- name: {0} obj: {1}".format (name, obj))
            for n, o in inspect.getmembers (obj):
                if (n.endswith ('_get') or n.endswith ('_post')):
                    print ("-- INVOKE: method-> {0} obj: {1}".format (n, o))

    
class Clinical (SwaggerEndpoint):

    def __init__(self, swagger_endpoint_url=""):
        self.url = swagger_endpoint_url

    '''
    http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/age/{age}/
    http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/age/{age}/sex/{sex}/
    http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/age/{age}/sex/{sex}/race/{race}/
    http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/ageMin/{ageMin}/ageMax/{ageMax}/limit/{limit}
    '''
    def get_patients (self, age, sex=None, race=None, location=None):
        """ Call the clinical API the old fashioned way until the Swagger spec is fixed.
        """
        result = None
        url = self.url
        if sex != None:
            url = "{0}/{1}".format (url, "sex/${sex}")
            if race != None:
                url = "{0}/{1}".format (url, "race/${race}")
                if age != None:
                    url = "{0}/{1}".format (url, "age/${age}")
                    if location != None:
                        url = "{0}/{1}".format (url, "location/${location}/")
                    
        #url = Template ("http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/age/${age}/sex/${sex}/race/${race}/location/${location}/")
        query_string = Template (url).substitute (age = age, sex = sex, race = race, location = location)

        # The following test is about convenience for developers, not security. Security is enforced at lower levels with IP
        # address filtering. This code only allows a sample record to be returned for testing purposes when we're not on the
        # translator.ncats.io machine.
        translator_ncats_io_mac_address = 2773026512788
        mac_address = get_mac()
        print ("Our mac address: {0}".format (mac_address))
        live_api_enabled = mac_address == translator_ncats_io_mac_address
        if live_api_enabled:
            print ("query: {}".format (query_string))
            try:            
                r = requests.get (query_string)
                r.raise_for_status()
                result = r.json ()
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                print (e)
        else:
            result = json.loads ("""[
         {
          "birth_date": "2006-08-02 00:00:00",
          "diag": {"ICD10:B08.4": {"2016-07-08 00:00:00": "OUTPATIENT"},
                   "ICD9:V19.2": {"2006-12-19 00:00:00": "OUTPATIENT"}},
          "geoCode": {"GEO:LAT": "35.22056", "GEO:LONG": "-80.69664"},
          "medList": {"MDCTN:10427": "2016-03-10 00:00:00",
                      "MDCTN:9502": "2017-01-20 00:00:00"},
          "patient_id": "32227752",
          "race": "white",
          "sex": "M"
         },
         {
          "birth_date": "2006-08-02 00:00:00",
          "diag": {"ICD10:B08.4": {"2016-07-08 00:00:00": "OUTPATIENT"},
                   "ICD9:V19.2": {"2006-12-19 00:00:00": "OUTPATIENT"}},
          "geoCode": {"GEO:LAT": "35.22056", "GEO:LONG": "-80.69664"},
          "medList": {"MDCTN:10427": "2016-03-10 00:00:00",
                      "MDCTN:9502": "2017-01-20 00:00:00"},
          "patient_id": "32227753",
          "race": "white",
          "sex": "M"
         }
        ]""")
        return result

class TestClinical(unittest.TestCase):

    pass

    '''
    exposures = Exposures ()

    def test_get_coordinates (self):
        print ("Test get coordinates")
        coordinates = self.exposures.get_coordinates (
            exposure_type='pm25',
            latitude='35.9131996',
            longitude='-79.0558445',
            radius='10')
        for coordinate in coordinates:
            print ("  --coord: lat: {0} lon: {1}".format (coordinate.latitude, coordinate.longitude))

    def test_get_scores (self):
        print ("Test get scores")
        scores = self.exposures.get_scores (
            exposure_type='pm25',
            start_date=date(2010, 1, 1),
            end_date=date(2010, 1, 7),
            exposure_point='35.9131996,-79.0558445')
        for exposure in scores:
            print ("  --scores: start: {0} end: {1} type: {2} lat: {3} lon: {4} units: {5} value: {6}".format (
                exposure.start_time, exposure.end_time, exposure.exposure_type,
                exposure.latitude, exposure.longitude, exposure.units, exposure.value))

    def test_get_values (self):
        print ("Test get values")
        values = self.exposures.get_values (
            exposure_type='pm25',
            start_date=date(2010, 1, 1),
            end_date=date(2010, 1, 7),
            exposure_point='35.9131996,-79.0558445')
        for exposure in values:
            print ("  --values: start: {0} end: {1} type: {2} lat: {3} lon: {4} units: {5} value: {6}".format (
                exposure.start_time, exposure.end_time, exposure.exposure_type,
                exposure.latitude, exposure.longitude, exposure.units, exposure.value))
    '''
    
if __name__ == '__main__':
    unittest.main()
