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
    def get_endotype (self, dob, model_type, race, sex, visits):
        return self.client.endotypes.endotypes_post (input={
            "date_of_birth": "2017-10-12",
            "model_type": "M0",
            "race": "1",
            "sex": "M",
            "visits": [
                {
                    "exposure": {
                        "exposure_type": "pm25",
                        "units": "ugm3",
                        "value": 33.3
                    },
                    "icd_codes": "ICD9:V12,ICD9:E002",
                    "lat": "20",
                    "lon": "20",
                    "time": "2017-10-12T21:12:29.451Z",
                    "visit_type": "INPATIENT"
                }
            ]
        }).result()

if __name__ == "__main__":
    conf = Config ('config.yaml')
    e = Endotype (conf.get_service("endotype")["url"])
    print (e.get_endotype (dob=None, model_type=None, race=None, sex=None, visits=None))
           
