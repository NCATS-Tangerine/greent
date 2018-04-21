import json
import requests
import traceback
from greent import node_types
from builder.lookup_utils import lookup_drug_by_name
from greent.service import Service
import logging
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

class BioNames(Service):
    """ BioNames services. """
    
    def __init__(self, context):
        """ Construct a bionames object and router for channeling searches. """
        super(BioNames, self).__init__("bionames", context)
        self.router = {
            "drug" : self._find_drug,
            "disease" : self._find_disease,
            "phenotype" : self._find_phenotype
        }
        
    def lookup(self, q, concept=None):
        """ Lookup a term with an optional concept. """
        result = []
        if concept:
            """ Route the search by concept. """
            result = self.router[concept](q, concept) if concept in self.router else []
        else:
            """ Try everything? Union the lot. """
            for route in self.router.items ():
                result = result + route(q, concept)
        logger.debug (f"search q: {q} results: {result}")
        return result
    
    def _find_drug(self, q, concept):
        ids = lookup_drug_by_name (q, self.context.core)
        return [ { "id" : i, "desc" : "" } for i in ids ] if ids else []
    
    def _find_disease(self, q, concept):
        return self._search_onto(q) + self._search_owlsim(q, concept)

    def _find_phenotype(self, q, concept):
        return self._search_onto(q)
    
    def _search_owlsim(self, q, concept):
        result = []
        try:
            owlsim_query = f"https://owlsim.monarchinitiative.org/api/search/entity/autocomplete/{q}?rows=20&start=0&category={concept}"
            logger.debug (f"owlsim query: {owlsim_query}")
            response = requests.get (owlsim_query).json ()
            logger.debug (f"owlsim response: {response}")
            if response and "docs" in response:
                result = [ { "id" : d["id"], "label" : ", ".join (d["label"]) } for d in response["docs"] ]
            logger.debug (f"owlsim result: {result}")
        except:
            traceback.print_exc ()
        return result
    
    def _search_onto(self, q):
        result = []
        try:
            result = self.context.core.onto.search (q, is_regex=True, full=True)
        except:
            traceback.print_exc ()
        return result
       
