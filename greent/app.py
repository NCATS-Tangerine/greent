from flask import Flask
from flask_graphql import GraphQLView
from greent.schema import Schema
from flask.views import View

from flask import request
from flask import Response
from flask import current_app
from pyld import jsonld
import json

class PatientStubView(View):
    ''' Fake static patient data for dev test environment. '''
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

class JSONLDView (GraphQLView):
    ''' Extend GraphQLView with ability to tag documents with JSON-LD context.
    https://antoniogarrote.wordpress.com/2016/11/23/graphql-is-for-silos/ '''
    def get_jsonld_context (self, context_id):
        context = None
        with open (context_id, "r") as stream:
            context = json.loads (stream.read ())
        return context
    def dispatch_request (self):
        response = super(JSONLDView, self).dispatch_request ()
        if isinstance (response, Response):
            context = self.get_jsonld_context ('greent/greent_context.json')
            doc = json.loads(response.get_data())
            expanded = jsonld.expand (doc['data'], { "expandContext" : context } )
            if True: #current_app.debug:
                print ("context: {}".format (json.dumps (context, indent=2)))
                print ("doc: {}".format (json.dumps (doc, indent=2)))
                print ("expanded: {}".format (json.dumps (expanded, indent=2)))
                print (" context-> {}".format (type(context)))
            print (" expanded-> {}".format (type(expanded)))
            #expanded = expanded[0]
            doc['data']['@context'] = context['@context']
            response.set_data(json.dumps (doc)) #json.dumps (expanded))
            #print (json.loads (response.get_data ()))            
        return response

def create_app(path='/graphql', **kwargs):
    app = Flask(__name__)
    app.debug = True
    app.add_url_rule(path, view_func=GraphQLView.as_view('graphql', schema=Schema, **kwargs))
    app.add_url_rule('/sgraphql', view_func=JSONLDView.as_view('sgraphql', schema=Schema, **kwargs))
    app.add_url_rule('/patients/', view_func=PatientStubView.as_view('patients'))
    return app

def main ():
    app = create_app(graphiql=True)
    app.run(host="0.0.0.0", threaded=True)

if __name__ == '__main__':
    main ()
