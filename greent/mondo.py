from ontobio.ontol_factory import OntologyFactory

#TODO: LOOKUP all the terms that map to this... or use an ancestor call that doesn't require such stuff (i.e. that handles this)
GENETIC_DISEASE=['DOID:630','http://purl.obolibrary.org/obo/EFO_0000508']
#GENETIC_DISEASE='EFO:0000508'
MONOGENIC_DISEASE='DOID:0050177'

class Mondo():
    """Class to hold/query the mondo ontology""" 
    def __init__(self):
        ofactory = OntologyFactory()
        self.ont = ofactory.create('mondo')
        #This seems to be required to make the ontology actually load:
        _ = self.ont.get_level(0)
    def get_doid(self,identifier):
        """We have an identifier, and we are going to use MONDO to try to convert it to a DOID"""
        upper_id = identifier.upper()
        obj_ids = self.get_mondo_id(upper_id)
        #Are any of the ids we get back a DOID?
        doids = []
        for obj_id in obj_ids:
            if obj_id.startswith('DOID:'):
                doids.append(obj_id)
        if len(doids) > 0:
            return doids
        #Didn't get anything, so get the xrefs and find DOIDS
        for obj_id in obj_ids:
            xref_ids = self.ont.xrefs(obj_id)
            for xref_id in xref_ids:
                if xref_id.startswith('DOID:'):
                    doids.append( xref_id )
        return doids
    def get_mondo_id(self,obj_id):
        """Given an id, find the main key(s) that mondo uses for the id"""
        if self.ont.has_node(obj_id):
            obj_ids = [obj_id]
        else:
            obj_ids = self.ont.xrefs(obj_id, bidirectional=True)
        return obj_ids
    def has_ancestor(self,obj, term):
        """Given an object and a term in MONDO, determine whether the term is an ancestor of the object.
        
        The object is a KNode representing a disease.
        Some complexity arises because the identifier for the node may not be the id of the concept in mondo.
        the XRefs in mondo are checked for the object if it is not intially found, but this may return more 
        than one entity if multiple mondo entries map to the same.  

        Returns: boolean representing whether any mondo objects derived from the subject have the term as an
                         ancestor.
                 The list of Mondo identifiers for the object, which have the term as an ancestor"""
        #TODO: The return signature is funky, fix it.
        obj_id = obj.identifier
        obj_ids = self.get_mondo_id(obj_id)
        return_objects=[]
        for obj_id in obj_ids:
            ancestors = self.ont.ancestors(obj_id)
            if GENETIC_DISEASE in ancestors:
                return_objects.append( obj_id )
        return len(return_objects) > 0, return_objects
    def is_genetic_disease(self,obj):
        """Checks mondo to find whether the subject has DOID:630 as an ancestor"""
        for g_disease in GENETIC_DISEASE:
            if self.has_ancestor(obj, g_disease):
                return True
        return False
    def is_monogenic_disease(self,obj):
        """Checks mondo to find whether the subject has DOID:0050177 as an ancestor"""
        return self.has_ancestor(obj, MONOGENIC_DISEASE)

def test():
    m = Mondo()
    from reasoner.graph_components import KNode,KEdge
    #alc_sens = KNode('OMIM:610251','D')
    #print(m.is_genetic_disease(alc_sens))
    huntingtons = KNode('DOID:12858','D')
    print(m.is_genetic_disease(huntingtons))

if __name__ == '__main__':
    test()
