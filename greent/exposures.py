import inspect
import json
import traceback
import unittest
from datetime import date
from datetime import datetime, timedelta

#from datetime import datetime, timedelta
#from dateutil.parser import parse as parse_date
import datetime

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

class Exposures(object):

    def __init__(self, swagger_endpoint_url="https://exposures.renci.org/v1/swagger.json"):
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

    def serialize (self, exposures):
        result = []
        for e in exposures:
            obj = {
                "exposure_type" : e.exposure_type,
                "latitude"      : e.latitude,
                "longitude"     : e.longitude,
                "start_time"    : e.start_time.strftime ("%Y-%m-%d"),
                "end_time"      : e.end_time.strftime ("%Y-%m-%d"),
                "value"         : e.value,
                "units"         : e.units
            }
            result.append (obj)
        return json.dumps (result)
            
    def get_coordinates (self, exposure_type, latitude, longitude, radius):
        return self.client.default.controllers_default_controller_exposures_exposure_type_coordinates_get (
            exposure_type=exposure_type,
            latitude=latitude,
            longitude=longitude,
            radius=radius).result ()

    def get_scores (self, exposure_type, start_date, end_date, exposure_point):
        #print ("exposures .............> {}".format (start_date))
         return self.serialize (self.client.default.controllers_default_controller_exposures_exposure_type_scores_get (
            exposure_type=exposure_type,
            start_date=start_date,
            end_date=end_date,
            exposure_point=exposure_point).result ())

    def get_values (self, exposure_type, start_date, end_date, exposure_point):
        return self.serialize (self.client.default.controllers_default_controller_exposures_exposure_type_values_get (
            exposure_type=exposure_type,
            start_date=start_date,
            end_date=end_date,
            exposure_point=exposure_point).result ())
            
class TestExposures(unittest.TestCase):

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

if __name__ == '__main__':
    unittest.main()
