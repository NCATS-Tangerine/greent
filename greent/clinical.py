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

class SwaggerEndpoint(object):

    def __init__(self, swagger_endpoint_url):
        http_client = RequestsClient ()
        # TODO: fix ssl certificate.
        http_client.session.verify = False
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
        pass


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

        url = "http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/age/${age}"
        
        if sex != None:
            url = "{0}/{1}".format (url, "sex/${sex}")
            if race != None:
                url = "{0}/{1}".format (url, "race/${race}")
                if location != None:
                    url = "{0}/{1}".format (url, "location/${location}/")
                    
        #url = Template ("http://tweetsie.med.unc.edu/CLINICAL_EXPOSURE/age/${age}/sex/${sex}/race/${race}/location/${location}/")
        query_string = Template (url).substitute (age = age, sex = sex, race = race, location = location)
        print ("query: {}".format (query_string))
        try:
            r = requests.get (query_string)
            r.raise_for_status()
            result = r.json ()
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print (e)
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
