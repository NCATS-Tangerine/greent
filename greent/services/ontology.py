from greent.service import Service
from greent.service import ServiceContext
import pronto
import re

class GenericOntology(Service):
    """ Sure, don't just dig around in obo files they say. But when the SPARQL is dry, we will drink straight from the obo if need be. """
    def __init__(self, context, obo):
        """ Load an obo file. """
        super(GenericOntology, self).__init__("go", context)
        self.ont = pronto.Ontology (obo)
    def label(self,identifier):
        """Return the label for an identifier"""
        return self.ont[identifier].name if identifier in self.ont else None
    def is_a(self,identifier, term):
        """Determine whether a term has a particular ancestor"""
        return term in self.ont[identifier].rparents() if identifier in self.ont else False
    def xrefs(self, identifier):
        """ Get external references. """
        result = []
        if identifier in self.ont:
            result = self.ont[identifier].other['xref']  if 'xref' in self.ont[identifier].other else []
        return result
    def synonyms(self, identifier, curie_pattern=None):
        """ Get synonyms. """
        return \
            [ x for x in self.ont[identifier].synonyms if curie_pattern and x.startswith(curie_pattern) ] + \
            [ syn for syn in self.ont[identifier].synonyms ] \
            if identifier in self.ont else []
    def search (self, text, is_regex=False):
        """ Search for the text, treating it as a regular expression if indicated. """
        result = []
        if is_regex:
            pat = re.compile (text)
            result = [ { "id" : term.id, "label" : term.name } for term in self.ont if pat.match (term.name) ]
        else:
            result = [ { "id" : term.id, "label" : term.name } for term in self.ont if text.lower() in term.name.lower() ]
        return result
