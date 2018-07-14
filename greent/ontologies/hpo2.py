from greent.services.onto import Onto
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging(__name__)

class HPO2(Onto):
    
    def __init__(self, context ):
        super(HPO2, self).__init__("hpo", context)
        self.name = 'HP' #Override

    def hp_get_synonym(self,identifier,curie_prefix):
        return super(HPO2,self).get_xrefs(identifier)

    def get_label(self,identifier):
        """Return the label for an identifier"""
        return super(HPO2,self).get_label(identifier)

    def get_hp_id(self,obj_id):
        return super(HPO2,self).get_xrefs(identifier)

    def has_ancestor(self,obj, terms):
        """ Is is_a(obj,t) true for any t in terms ? """
        ids = self.get_mondo_id(obj.identifier)        
        results = [ i for i in ids for candidate_ancestor in terms if super(Mondo2,self).is_a(i, candidate_ancestor) ] \
                 if terms else []
        return len(results) > 0, results

    def substring_search(self,name):
        ciname = '(?i){}'.format(name)
        results = super(HPO2,self).search(ciname,synonyms=True,is_regex=True)

    def case_insensitive_search(self,name):
        ciname = '(?i)^{}$'.format(name)
        return super(HPO2,self).search(ciname,synonyms=True,is_regex=True)

    def search(self, name):
        results = super(HPO2,self).search(name)
        if len(results) == 0:
            if ',' in name:
                parts =name.split(',')
                parts.reverse()
                ps = [p.strip() for p in parts]
                newname = ' '.join(ps)
                results = super(HPO2,self).search(newname)
        return results
    


