import logging
import unittest

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

    def test_exposures (self):
        exposure_type = "pm25"
        start_date = "2010-1-1",
        end_date = "2010-1-7",
        lat = "35.9131996"
        lon = "-79.0558445"
        exposure_point = ",".join ([ lat, lon ]) #"35.9131996,-79.0558445"
        
        results = self.greenT.get_exposure_scores (exposure_type, start_date, end_date, exposure_point)
        for r in results.data ['exposureScores']:
            this_lat = str(r['latitude'])
            #print ("-------- ({}) ({})".format (this_lat, lat))
            self.assertTrue (this_lat == lat)

        #self.greenT.print_exposure_results (results, key='exposureScores')
        
        results = self.greenT.get_exposure_values (exposure_type, start_date, end_date, exposure_point)
        for r in results.data ['exposureValues']:
            this_lat = str(r['latitude'])
            #print ("-------- ({}) ({})".format (this_lat, lat))
            self.assertTrue (this_lat == lat)

        #self.greenT.print_exposure_results (results, key='exposureValues')

    def test_chembio (self):
        chemicals = [ 'D052638' ]
        conditions = self.greenT.get_exposure_conditions (chemicals)
        t = False
        for c in conditions:
            if c["gene"] == "http://chem2bio2rdf.org/uniprot/resource/gene/IL6":
                t = True
                break
        self.assertTrue (t)
        # print (json.dumps (conditions, indent=2))
        drugs = self.greenT.get_drugs_by_condition (conditions=[ "d001249" ])
        # print (json.dumps (drugs, indent=2))
        for d in [ "Paricalcitol", "NIMESULIDE", "Ramipril" ]:
            self.assertTrue (d in drugs)

        #paths = self.greenT.get_genes_pathways_by_disease (diseases)
        #print (paths)

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

