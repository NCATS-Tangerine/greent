from SPARQLWrapper import SPARQLWrapper2, JSON
from string import Template
import os
from greent.util import LoggingUtil
from pprint import pprint

logger = LoggingUtil.init_logging (__file__)
#import logging
#logger = LoggingUtil.init_logging (__file__, logging.DEBUG)


class TripleStore(object):
    """ Connect to a SPARQL endpoint and provide services for loading and executing queries."""
    def __init__(self, hostname):
        self.service =  SPARQLWrapper2 (hostname)

    #@provenance()
    def get_template (self, query_name):
        return Template (self.get_template_text (query_name))
    def get_template_text (self, query_name):
        query = None
        fn = os.path.join(os.path.dirname(__file__), 'query',
            '{0}.sparql'.format (query_name))
        with open (fn, 'r') as stream:
            query = stream.read ()
            #logger.debug ('query template: %s', query)
        return query
    def execute_query (self, query):
        """ Execute a SPARQL query.

        :param query: A SPARQL query.
        :return: Returns a JSON formatted object.
        """
        print (query)
        self.service.setQuery (query)
        self.service.setReturnFormat (JSON)
        return self.service.query().convert ()
    
    def query (self, query_text, outputs, flat=False):
        logger.debug (query_text)
        print (query_text)
        response = self.execute_query (query_text)
        result = None
        if flat:
            result = list(map(lambda b : [ b[val].value for val in outputs ], response.bindings ))
        else:
            result = list(map(lambda b : { val : b[val].value for val in outputs }, response.bindings ))
        logger.debug ("query result: %s", result)
        return result
    def query_template (self, template_text, outputs, inputs=[]):
        return self.query (Template (template_text).safe_substitute (**inputs), outputs)
    def query_template_file (self, template_file, outputs, inputs=[]):
        return self.query (self.get_template_text (template), inputs, outputs)
