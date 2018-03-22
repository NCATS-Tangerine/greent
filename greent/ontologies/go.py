from greent.service import Service
from ontobio.ontol_factory import OntologyFactory

CELLULAR_COMPONENT='GO:0005575'
MOLECULAR_FUNCTION='GO:0003674'
BIOLOGICAL_PROCESS='GO:0008150'

#TODO: move commonalities here and with mondo into somehwere
class GO(Service):
    
    """ A pragmatic class to query the gene  ontology. Until better sources emerge, we roll our own. """ 
    def __init__(self, context ):
        super(GO, self).__init__("go", context)
        ofactory = OntologyFactory()
        try:
            #sometimes the ontology world is down :(
            self.ont = ofactory.create('go')
        except:
            self.ont = ofactory.create('obo:go')
        #self.ont = ofactory.create('./mondo.owl')
        #This seems to be required to make the ontology actually load:
        _ = self.ont.get_level(0)
        
    def get_label(self,identifier):
        """Return the label for an identifier"""
        return self.ont.label(identifier)

    def has_ancestor(self,identifier, term):
        """Determine whether a term has a particular ancestor"""
        #TODO: The return signature is funky, fix it.
        ancestors = self.ont.ancestors(identifier)
        return term in ancestors

    def is_cellular_component(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.has_ancestor(identifier, CELLULAR_COMPONENT)
    def is_biological_process(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.has_ancestor(identifier, BIOLOGICAL_PROCESS)
    def is_molecular_function(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.has_ancestor(identifier, MOLECULAR_FUNCTION)

