from greent.service import Service
from greent.service import ServiceContext
from ontobio.ontol_factory import OntologyFactory
import pronto
import re

CELLULAR_COMPONENT='GO:0005575'
MOLECULAR_FUNCTION='GO:0003674'
BIOLOGICAL_PROCESS='GO:0008150'

#TODO: move commonalities here and with mondo into somehwere
class GO2(Service):
    
    """ A pragmatic class to query the gene  ontology. Until better sources emerge, we roll our own. """ 
    def __init__(self, context ):
        super(GO2, self).__init__("go", context)
        self.ont = pronto.Ontology ('go.obo')
            
    def label(self,identifier):
        """Return the label for an identifier"""
        return self.ont[identifier].name if identifier in self.ont else None

    def is_a(self,identifier, term):
        """Determine whether a term has a particular ancestor"""
        return term in self.ont[identifier].rparents() if identifier in self.ont else False

    def xrefs(self, identifier):
        result = []
        if identifier in self.ont:
            result = self.ont[identifier].other['xref']  if 'xref' in self.ont[identifier].other else []
        return result

    def synonyms(self, identifier, curie_pattern=None):
        return \
            [ x for x in self.ont[identifier].synonyms if curie_pattern and x.startswith(curie_pattern) ] + \
            [ syn for syn in self.ont[identifier].synonyms ] \
            if identifier in self.ont else []

    def search (self,text, is_regex=False):
        result = []
        if is_regex:
            pat = re.compile (text)
            result = [ term.name for term in self.ont if pat.match (term.name) ]
        else:
            result = [ term.name for term in self.ont if text is term.name ]
        return result

    def is_cellular_component(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.has_ancestor(identifier, CELLULAR_COMPONENT)
    def is_biological_process(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.has_ancestor(identifier, BIOLOGICAL_PROCESS)
    def is_molecular_function(self,identifier):
        """Checks go to find whether the subject is a cellular component"""
        return self.has_ancestor(identifier, MOLECULAR_FUNCTION)

            
if __name__ == "__main__":
    g = GO2(ServiceContext.create_context ())
    print (g.label ("GO:2001317"))
    print (g.is_a("GO:2001317", "GO:1901362"))
    print (g.search ("kojic acid biosynthetic process"))
    print (g.search ("ko.*c", is_regex=True))
    print (g.xrefs("GO:2001317"))
    print (g.synonyms("GO:2001317"))
    
    x = """
    "GO:2001317": {
        "desc": "The chemical reactions and pathways resulting in the formation of kojic acid.",
        "id": "GO:2001317",
        "name": "kojic acid biosynthetic process",
        "other": {
            "created_by": [
                "rfoulger"
            ],
            "creation_date": [
                "2012-04-18T09:22:46Z"
            ],
            "id": [
                "GO:2001317"
            ],
            "is_a": [
                "GO:0018130 ! heterocycle biosynthetic process",
                "GO:0034309 ! primary alcohol biosynthetic process",
                "GO:0042181 ! ketone biosynthetic process",
                "GO:1901362 ! organic cyclic compound biosynthetic process",
                "GO:2001316 ! kojic acid metabolic process"
            ],
            "namespace": [
                "biological_process"
            ]
        },
        "relations": {
            "is_a": [
                "GO:0018130",
                "GO:0034309",
                "GO:0042181",
                "GO:1901362",
                "GO:2001316"
            ]
        }
    }
    """
