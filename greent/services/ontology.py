import pronto
import re
import logging
from greent.util import LoggingUtil
from greent.service import Service
from pronto.relationship import Relationship

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

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
        print (f"is {identifier} a {term}?")
        is_a = False
        is_a_rel = Relationship('is_a')
        if identifier in self.ont:
            #parents = self.ont[identifier].parents
            the_term = self.ont[identifier]
            parents = the_term.relations[is_a_rel] if is_a_rel in the_term.relations else []
            print (f"{identifier} parents {parents}")
            for ancestor in parents:
                ancestor_id = ancestor.id
                if ' ' in ancestor.id:
                    ancestor_id = ancestor.id.split(' ')[0]
                print (f"   ancestor: {ancestor_id}")
                is_a = ancestor_id == term
                if is_a:
                    break
                if 'xref' in ancestor.other:
                    for xancestor in ancestor.other['xref']:
                        print (f"      ancestor-xref: {xancestor} ?=~ {term}")
                        is_a = xancestor.startswith (term)
                        if is_a:
                            break
                if not is_a:
                    is_a = self.is_a (ancestor_id, term)
                if is_a:
                    break
        print (f"{identifier} is_a {term} => {is_a}")
        return is_a
    def xrefs(self, identifier):
        """ Get external references. """
        result = []
        if identifier in self.ont:
            result = self.ont[identifier].other['xref']  if 'xref' in self.ont[identifier].other else []
        result = [ x.split(' ') if ' ' in x else [x, ''] for x in result ]
        result = [ { 'id' : x[0], 'desc' : x[1] } for x in result if len(x) == 2 and ':' in x[1] ]
        return result
    def synonyms(self, identifier, curie_pattern=None):
        """ Get synonyms. """
        return \
            [ x for x in self.ont[identifier].synonyms if curie_pattern and x.startswith(curie_pattern) ] + \
            [ syn for syn in self.ont[identifier].synonyms ] \
            if identifier in self.ont else []
    def search (self, text, is_regex=False, ignore_case=True):
        """ Search for the text, treating it as a regular expression if indicated. """
        print (f"text: {text} is_regex: {is_regex}, ignore_case: {ignore_case}")
        pat = None
        if is_regex:
            pat = re.compile(text, re.IGNORECASE) if ignore_case else re.compile(text)
        result = {}
        for term in self.ont:
            if is_regex:
                if pat.match (term.name):
                    logger.debug (f"  matched {text} pattern in term name: {term.name}")
                    result[term.id] = term
                else:
                    for syn in term.synonyms:
                        if pat.match (syn.desc):
                            logger.debug (f"  matched {text} pattern in synonym: {syn.desc}")
                            result[term.id] = term
            else:
                if text.lower() == term.name.lower():
                    logger.debug (f"  text {text} == term name {term.name}")
                    result[term.id] = term
                else:
                    for syn in term.synonyms:
                        if text.lower() == syn.desc.lower():
                            logger.debug (f"  text {text.lower()} == synonym: {syn.desc.lower()}")
                            result[term.id] = term
        result = [  { "id" : term.id, "label" : term.name } for key, term in result.items () ]
        return result
    
    def lookup(self, identifier):
        """ Given an identifier, find ids in the ontology for which it is an xref. """
        assert identifier and ':' in identifier, "Must provide a valid identifier."
        result = []
        for term in self.ont:
            xrefs = []
            if 'xref' in term.other:
                for xref in term.other['xref']:
                    if xref.startswith (identifier):
                        if ' ' in xref:
                            xref_pair = xref.split(' ')
                            xref_pair = [ xref_pair[0], ' '.join (xref_pair[1:]) ]
                        else:
                            xref_pair = [xref, '']
                            print (f"xref_pair: {xref_pair}")
                        xrefs.append ({
                            'id'   : xref_pair[0],
                            'desc' : xref_pair[1]
                        })
            if len(xrefs) > 0:
                result.append ({
                    "id"    : term.id,
                    "xrefs" : xrefs
                })                
        return result
        
