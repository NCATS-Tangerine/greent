import logging
import unittest
import json
from pprint import pprint, pformat
from core import GreenT

class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)

logger = LoggingUtil.init_logging (__file__)

class TestGreenT(unittest.TestCase):

    greenT = GreenT ()

    def test_exposure_scores (self):
        exposure_type = "pm25"
        start_date = "2010-1-1"
        end_date = "2010-1-7"
        lat = "35.9131996"
        lon = "-79.0558445"
        exposure_point = ",".join ([ lat, lon ])
        results = self.greenT.get_exposure_scores (exposure_type, start_date, end_date, exposure_point)
        for r in results['scores']:
            this_lat, this_lon = r['latLon'].split (',')
            print ("   -- lat: (out:{0}) (in:{1}) lon: (out:{2}) (in:{3}) score: {4}".format (
                this_lat, lat, this_lon, lon, r['score']))
            self.assertTrue (this_lat == lat and this_lon == lon)
        
    def test_exposure_values (self):
        exposure_type = "pm25"
        start_date = "2010-1-1"
        end_date = "2010-1-7"
        lat = "35.9131996"
        lon = "-79.0558445"
        exposure_point = ",".join ([ lat, lon ])
        results = self.greenT.get_exposure_values (exposure_type, start_date, end_date, exposure_point)
        for r in results['values']:
            this_lat, this_lon = r['latLon'].split (',')
            print ("   -- lat: (out:{0}) (in:{1}) lon: (out:{2}) (in:{3}) value: {4}".format (
                this_lat, lat, this_lon, lon, r['value']))
            self.assertTrue (this_lat == lat and this_lon == lon)

    def test_chembio (self):
        chemicals = [ 'D052638' ]
        conditions = self.greenT.get_exposure_conditions (chemicals)
        t = False
        for c in conditions:
            if c["gene"] == "http://chem2bio2rdf.org/uniprot/resource/gene/IL6":
                t = True
                break
        self.assertTrue (t)
        print (json.dumps (conditions[:2], indent=2))
        drugs = self.greenT.get_drugs_by_condition (conditions=[ "d001249" ])
        for d in [ "Paricalcitol", "NIMESULIDE", "Ramipril" ]:
            self.assertTrue (d in drugs)

    def test_clinical (self):
        '''
        104855982
        104855982
        104855982
        100493218
        '''

        '''
        result = self.greenT.clinical.get_patients (age='4', sex='male', race='white', location='OUTPATIENT')
        result = sorted (result, key = lambda e : e['patient_id'] )
        self.assertTrue ( result[0]['patient_id'] == '104855982' )

        result = self.greenT.clinical.get_patients (age='4', sex='male', race='white')
        result = sorted (result, key = lambda e : e['patient_id'] )
        self.assertTrue ( result[0]['patient_id'] == '104855982' )


        result = self.greenT.clinical.get_patients (age='4', sex='male')
        result = sorted (result, key = lambda e : e['patient_id'] )
        self.assertTrue ( result[0]['patient_id'] == '104855982' )
        
        result = self.greenT.clinical.get_patients (age='4')
        result = sorted (result, key = lambda e : e['patient_id'] )
        self.assertTrue (result[0]['patient_id'] ==  '100493218' )
        '''
        pass
    
if __name__ == '__main__':
    unittest.main()

