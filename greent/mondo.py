from greent.service import Service
from greent.service import ServiceContext
from ontobio.ontol_factory import OntologyFactory
from greent.graph_components import KNode, KEdge
from greent import node_types
from cachier import cachier
import datetime

#TODO: LOOKUP all the terms that map to this... or use an ancestor call that doesn't require such stuff (i.e. that handles this)
GENETIC_DISEASE=('DOID:630','http://purl.obolibrary.org/obo/EFO_0000508')
#GENETIC_DISEASE='EFO:0000508'
MONOGENIC_DISEASE='DOID:0050177'

class Mondo(Service):
    
    """ A pragmatic class to query the mondo ontology. Until better sources emerge, we roll our own. """ 
    def __init__(self, context ):
        super(Mondo, self).__init__("mondo", context)
        ofactory = OntologyFactory()
        try:
            #sometimes the ontology world is down :(
            self.ont = ofactory.create('mondo')
        except:
            try:
                self.ont = ofactory.create('obo:mondo')
            except:
                self.ont = ofactory.create('onto_cache/mondo.owl')
        #self.ont = ofactory.create('./mondo.owl')
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

    def get_label(self,identifier):
        """Return the label for an identifier"""
        obj_ids = self.get_mondo_id(identifier)
        return self.ont.label(obj_ids[0])

    def get_mondo_id(self,obj_id):
        """Given an id, find the main key(s) that mondo uses for the id"""
        if self.ont.has_node(obj_id):
            obj_ids = [obj_id]
        else:
            obj_ids = self.ont.xrefs(obj_id, bidirectional=True)
        return obj_ids

    def has_ancestor(self,obj, terms):
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
            for term in terms:
                if term in ancestors:
                    return_objects.append( obj_id )
        return len(return_objects) > 0, return_objects

    def is_genetic_disease(self,obj):
        """Checks mondo to find whether the subject has DOID:630 as an ancestor"""
        return self.has_ancestor(obj, GENETIC_DISEASE)

    def is_monogenic_disease(self,obj):
        """Checks mondo to find whether the subject has DOID:0050177 as an ancestor"""
        return self.has_ancestor(obj, MONOGENIC_DISEASE)

    #@cachier(stale_after=datetime.timedelta(days=20))
    def doid_get_genetic_condition (self, disease):
        """Given a gene specified as an HGNC curie, return associated genetic conditions.
        A genetic condition is specified as a disease that descends from a ndoe for genetic disease in MONDO."""
        relations = []
        is_genetic_condition, new_object_ids = self.is_genetic_disease (disease)
        orphanet_prefix = "http://www.orpha.net/ORDO/Orphanet_"
        if is_genetic_condition:
            for new_object_id in new_object_ids:
                if new_object_id.startswith (orphanet_prefix):
                    new_object_id = new_object_id.replace (orphanet_prefix, 'ORPHANET.GENETIC_CONDITION:')
                elif new_object_id.startswith ('DOID:'):
                    new_object_id = new_object_id.replace ('DOID:', 'DOID.GENETIC_CONDITION:')
                relations.append ( (self.get_edge ({}, 'is_genetic_condition'), KNode (new_object_id, node_types.GENETIC_CONDITION) ))
        return relations

    def doid_get_orphanet_genetic_condition (self, disease):
        results = self.doid_get_genetic_condition (disease)
        return [ r for r in results if r[1].identifier.startswith ('ORPHANET.GENETIC_CONDITION') ]
    
    def doid_get_doid_genetic_condition (self, disease):
        results = self.doid_get_genetic_condition (disease)
        return [ r for r in results if r[1].identifier.startswith ('DOID.GENETIC_CONDITION') ]
    
def test():
    m = Mondo (ServiceContext.create_context ())
    alc_sens = KNode('OMIM:610251',node_types.DISEASE)
    print(m.is_genetic_disease(alc_sens))
    print('------')
    huntingtons = KNode('DOID:12858',node_types.DISEASE)
    print(m.is_genetic_disease(huntingtons))
    tests = [ "DOID:8545", "OMIM:218550", "OMIM:234000", "DOID:0060334", "DOID:0050524", "DOID:0060599", "DOID:12858" ]
    for t in tests:
        print (m.doid_get_orphanet_genetic_condition (KNode (t, node_types.DISEASE)))
        print (m.doid_get_doid_genetic_condition (KNode (t, node_types.DISEASE)))
    
if __name__ == '__main__':
    test()
