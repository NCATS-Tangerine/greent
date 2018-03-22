from greent.service import Service
from greent.service import ServiceContext
from ontobio.ontol_factory import OntologyFactory
from greent.graph_components import KNode, KEdge
from greent.util import LoggingUtil
from greent import node_types
from cachier import cachier
import datetime


logger = LoggingUtil.init_logging (__file__)

#TODO: move some some of the common stuff between here and Mondo into an ontology class/module.
class HPO(Service):
    
    """ A pragmatic class to query the mondo ontology. Until better sources emerge, we roll our own. """ 
    def __init__(self, context ):
        super(HPO, self).__init__("hpo", context)
        ofactory = OntologyFactory()
        try:
            #sometimes the ontology world is down :(
            self.ont = ofactory.create('hp')
        except:
            logger.warn('Problem reaching sparql endpoint, falling back to obo')
            try:
                self.ont = ofactory.create('obo:hp')
            except:
                logger.error('Problem reaching obo, add local owl file')
                import sys
                sys.exit(1)
                #self.ont = ofactory.create('onto_cache/hpo.owl')
        #This seems to be required to make the ontology actually load:
        _ = self.ont.get_level(0)
        

    def hp_get_synonym(self,hp_identifier,curie_prefix):
        xref_ids = self.ont.xrefs(hp_identifier)
        doids = []
        for xref_id in xref_ids:
            if xref_id.startswith(curie_prefix):
                doids.append( xref_id )
        return doids

    def get_label(self,identifier):
        """Return the label for an identifier"""
        obj_ids = self.get_hp_id(identifier)
        return self.ont.label(obj_ids[0])

    def get_hp_id(self,obj_id):
        """Given an id, find the main key(s) that mondo uses for the id"""
        if self.ont.has_node(obj_id):
            obj_ids = [obj_id]
        else:
            obj_ids = self.ont.xrefs(obj_id, bidirectional=True)
        return obj_ids

    def has_ancestor(self,obj, terms):
        """Given an object and a term in MONDO, determine whether the term is an ancestor of the object.
        
        The object is a KNode representing a disease.
        Some complexity arises because the identifier for the node may not be the id of the concept in hp.
        the XRefs in hp are checked for the object if it is not intially found, but this may return more 
        than one entity if multiple hp entries map to the same.  

        Returns: boolean representing whether any hp objects derived from the subject have the term as an
                         ancestor.
                 The list of Mondo identifiers for the object, which have the term as an ancestor"""
        #TODO: The return signature is funky, fix it.
        obj_id = obj.identifier
        obj_ids = self.get_hp_id(obj_id)
        return_objects=[]
        for obj_id in obj_ids:
            ancestors = self.ont.ancestors(obj_id)
            for term in terms:
                if term in ancestors:
                    return_objects.append( obj_id )
        return len(return_objects) > 0, return_objects

    def substring_search(self,name):
        ciname = '(?i){}'.format(name)
        results = self.ont.search(ciname,synonyms=True,is_regex=True)

    def case_insensitive_search(self,name):
        ciname = '(?i)^{}$'.format(name)
        return self.ont.search(ciname,synonyms=True,is_regex=True)

    def search(self, name):
        #Exact match 
        results = self.case_insensitive_search(name)
        if len(results) == 0:
            if ',' in name:
                parts =name.split(',')
                parts.reverse()
                ps = [p.strip() for p in parts]
                newname = ' '.join(ps)
                results = self.case_insensitive_search(newname)
        return results




