import json
import traceback
import unittest
import datetime
from string import Template
from pprint import pprint
from greent.util import SwaggerEndpoint
from greent.util import LoggingUtil
from greent.util import Config

class Endotype(SwaggerEndpoint):
    def __init__(self, url):
        super(Endotype, self).__init__(url)
    @staticmethod
    def create_request (dob, model_type, race, sex, visits):
        return {
            "date_of_birth": dob,
            "model_type": model_type,
            "race": race,
            "sex": sex,
            "visits": visits
        }
    @staticmethod
    def create_exposure (exposure_type, units, value):
        return {
            "exposure_type": exposure_type,
            "units": units,
            "value": value
        }
    @staticmethod
    def create_visit (icd_codes, lat, lon, time, visit_type, exposures):
        return {
            "exposures"  : exposures,
            "icd_codes"  : icd_codes,
            "lat"        : lat,
            "lon"        : lon,
            "time"       : time,
            "visit_type" : visit_type
        }
    def get_endotype (self, request):
        print (json.dumps (request))
        r = self.client.endotypes.endotypes_post (input=request).result()
        print (r)
        return r['output'] if 'output' in r else None
    
if __name__ == "__main__":
    conf = Config ('config.yaml')
    e = Endotype (conf.get_service("endotype")["url"])
    exposures = list(map(lambda exp : e.create_exposure (**exp), [{
        "exposure_type": "pm25",
        "units"        : "",
        "value"        : 2
    }]))
    visits = list(map(lambda v : e.create_visit(**v), [{
            "icd_codes"  : "ICD9:V12,ICD9:E002",
            "lat"        : "20",
            "lon"        : "20",
            "time"       : "2017-10-12 21:12:29",
            "visit_type" : "INPATIENT",
            "exposures"  : exposures
        }]))
    request = e.create_request (dob= "2017-10-04", model_type="M0", race="1", sex="M", visits = visits)
    print (json.dumps (e.get_endotype (request), indent=2))
           
