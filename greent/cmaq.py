import inspect
import json
import requests
import traceback
import unittest
import datetime
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from string import Template
from pprint import pprint
from greent.swagger import SwaggerEndpoint
from greent.service import ServiceContext

class CMAQ (SwaggerEndpoint):

    def __init__(self, context):
        super (CMAQ, self).__init__("cmaq", context)
        self.url = self.url
    
    def get_meta (self):
        return self.client.cmaq.get_cmaq ().result (timeout = 10)
    
    def get_scores (self, start_date, end_date, lat_lon, exposure_type='pm25', resolution='7day', aggregation='max', utcOffset='utc'):
        return self.client.cmaq.get_cmaq_getScores (
            exposureType = exposure_type,
            startDate = datetime.datetime.strptime(start_date, "%Y-%m-%d").date (),
            endDate = datetime.datetime.strptime(end_date, "%Y-%m-%d").date (),
	    latLon = lat_lon,
            resolution = resolution,
            aggregation = aggregation,
            utcOffset = utcOffset).result (timeout = 10)

    def get_values (self, start_date, end_date, lat_lon, exposure_type='pm25', resolution='7day', aggregation='max', utcOffset='utc'):
        return self.client.cmaq.get_cmaq_getValues (
            exposureType = exposure_type,
            startDate = datetime.datetime.strptime(start_date, "%Y-%m-%d").date (),
            endDate = datetime.datetime.strptime(end_date, "%Y-%m-%d").date (),
	    latLon = lat_lon,
            resolution = resolution,
            aggregation = aggregation,
            utcOffset = utcOffset).result ()

#c = CMAQ ("https://exposures.renci.org/v1/swagger.json")
#c = CMAQ ("https://raw.githubusercontent.com/RENCI/nih-exposures-api/master/specification/swagger.yml")


#c = CMAQ ("https://app.swaggerhub.com/apiproxy/schema/file/mjstealey/environmental_exposures_api/0.0.1/swagger.json")
#c.inspect ()

#pprint (c.get_scores (start_date = "2011-01-01",
#                      end_date = "2011-12-31",
#                      lat_lon = "35.9131996,-79.0558445"))

#pprint (c.get_values (start_date = "2011-01-01",
#                      end_date = "2011-12-31",
#                      lat_lon = "35.9131996,-79.0558445"))

'''
{
  "cmaq": [
    {
      "aggregation": [
        [
          "max",
          "avg"
        ]
       ],
      "endDate": "2011-12-31",
      "exposureType": "pm25",
      "exposureUnit": "ugm3",
      "resolution": [
        [
          "hour",
          "day",
          "7day",
          "14day"
        ]
      ],
      "startDate": "2011-01-01"
    },
    {
      "aggregation": [
        [
          "max",
          "avg"
        ]
      ],
      "endDate": "2011-12-31",
      "exposureType": "o3",
      "exposureUnit": "ppm",
      "resolution": [
        [
          "hour",
          "day",
          "7day",
          "14day"
        ]
      ],
      "startDate": "2011-01-01"
    }
  ]
}


{
  "values": [
    {
      "dateTime": "2011-01-01T00:00:00+00:00",
      "latLon": "\"35.9131996,-79.0558445\"",
      "value": 30.3144
    },
    {
      "dateTime": "2011-01-02T00:00:00+00:00",
      "latLon": "\"35.9131996,-79.0558445\"",
      "value": 14.7758
    },
'''
