from greent.services.onto import Onto

CELLULAR_COMPONENT='GO:0005575'
MOLECULAR_FUNCTION='GO:0003674'
BIOLOGICAL_PROCESS='GO:0008150'

#TODO: move commonalities here and with mondo into somehwere
class GO2(Onto):
    
    """ A pragmatic class to query the gene  ontology. Until better sources emerge, we roll our own. """ 
    def __init__(self, context):
        super(GO2, self).__init__("go", context)
            
    def label(self,identifier):
        """Return the label for an identifier"""
        return super(GO2,self).get_label(identifier)

    def is_a(self,identifier, term):
        """Determine whether a term has a particular ancestor"""
        return super(GO2,self).is_a(identifier,term)

    def xrefs(self, identifier):
        return super(GO2,self).get_xrefs(identifier)

    def synonyms(self, identifier, curie_pattern=None):
        return super(GO2,self).get_synonyms(identifier)

    def search (self,text, is_regex=False):
        return super(GO2,self).searh (text, is_regex)

    def is_cellular_component(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.is_a(identifier, CELLULAR_COMPONENT)
    def is_biological_process(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.is_a(identifier, BIOLOGICAL_PROCESS)
    def is_molecular_function(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.is_a(identifier, MOLECULAR_FUNCTION)
            
