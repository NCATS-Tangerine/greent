from flask import Flask
from flask_graphql import GraphQLView
from greent.schema import Schema
from flask.views import View

class PatientStubView(View):
    def dispatch_request(self):
        return """
        [
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
        ]
        """

def create_app(path='/graphql', **kwargs):
    app = Flask(__name__)
    app.debug = True
    app.add_url_rule(path, view_func=GraphQLView.as_view('graphql', schema=Schema, **kwargs))
    app.add_url_rule('/patients/', view_func=PatientStubView.as_view('patients'))
    return app

def main ():
    app = create_app(graphiql=True)
    app.run(host="0.0.0.0", threaded=True)

if __name__ == '__main__':
    main ()
