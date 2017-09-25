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
        #http_client.session.verify = False
        self.client = SwaggerClient.from_url(
            swagger_endpoint_url,
            http_client=http_client)

    def inspect (self):
        for name, obj in inspect.getmembers (self.client):
            print ("-- name: {0} obj: {1}".format (name, obj))
            for n, o in inspect.getmembers (obj):
                if (n.endswith ('_get') or n.endswith ('_post')):
                    print ("-- INVOKE: method-> {0} obj: {1}".format (n, o))
    
class CMAQ (SwaggerEndpoint):

    def __init__(self, swagger_endpoint_url=""):
        super (CMAQ, self).__init__(swagger_endpoint_url)
        self.url = swagger_endpoint_url
    def get_scores (self, start_date, end_date, lat_lon, exposureType='pm25', resolution='7day', aggregation='max', utcOffset='utc'):
        return self.client.default.controllers_default_controller_cmaq_get_scores_get (
            exposureType = exposureType,
            startDate = start_date,
            endDate = end_date, 
	    latLon = lat_lon,
            resolution = resolution,
            aggregation = aggregation,
            utcOffset = utcOffset).result ()

            
# controllers_default_controller_cmaq_get
# controllers_default_controller_cmaq_get_scores_get
# controllers_default_controller_cmaq_get_values_get

c = CMAQ ("http://apps-ffs.renci.org:5000/v1/swagger.json")

c.inspect ()

print (c.get_scores (start_date = date (2011, 1, 1),
                     end_date = date (2011, 12, 31),
                     lat_lon = "35.9131996,-79.0558445"))
