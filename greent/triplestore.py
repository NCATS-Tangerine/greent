from SPARQLWrapper import SPARQLWrapper2, JSON

class TripleStore(object):
    """ Connect to a SPARQL endpoint and provide services for loading and executing queries."""
    def __init__(self, hostname):
        self.service =  SPARQLWrapper2 (hostname)

    #@provenance()
    def execute_query (self, query):
        """ Execute a SPARQL query.

        :param query: A SPARQL query.
        :return: Returns a JSON formatted object.
        """
        self.service.setQuery (query)
        self.service.setReturnFormat (JSON)
        return self.service.query().convert ()
    def execute_query_file (self, query_file):
        """ Execute a SPARQL query based on a file.

        :param query_file: The file containing th query.
        :return: Returns a JSON formatted object.
        """
        result = None
        with open (query_file, "r") as stream:
            query = stream.read ()
            result = self.execute_query (query)
        return result
