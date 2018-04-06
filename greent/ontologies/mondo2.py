import requests
from greent.services.onto import Onto
from greent.service import Service
from greent.graph_components import KNode, KEdge
from greent.util import LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging (__file__)

#TODO: LOOKUP all the terms that map to this... or use an ancestor call that doesn't require such stuff (i.e. that handles this)
GENETIC_DISEASE=('MONDO:0021198', 'DOID:630', 'EFO:0000508', 'MONDO:0003847')
MONOGENIC_DISEASE='DOID:0050177'

#class Mondo(Service):
class Mondo2(Onto):
    
    """ Query MONDO """
    def __init__(self, context):
        super(Mondo2, self).__init__("mondo", context)
    
    def get_doid(self,identifier):
        return self.get_xrefs (identifier, filter=['DOID:'])
    
    def mondo_get_doid(self,mondo_identifier):
        return self.mondo_get_synonym(mondo_identifier,'DOID')

    def mondo_get_umls(self,mondo_identifier):
        return self.mondo_get_synonym(mondo_identifier,'UMLS')

    def mondo_get_efo(self,mondo_identifier):
        return self.mondo_get_synonym(mondo_identifier,'EFO')

    def mondo_get_synonym(self, identifier, prefix):
        """ Return external references optionally filtered by prefix. """
        return self.get_xrefs (identifier, filter=[prefix])
    
    def get_label(self,identifier):
        return super(Mondo2,self).get_label(identifier)
    
    def get_mondo_id(self,obj_id):
        result = []
        label = super(Mondo2,self).get_label(obj_id)
        if label and 'label' in label and lable['label'] is not None:
            logger.debug (f"input id {obj_id} is a MONDO id.")
            result.append (obj_id)
        else:
            result = super(Mondo2,self).lookup(obj_id)
            logger.debug (f"input id {obj_id} resolves to id {result}")
        return result
    
    def has_ancestor(self,obj, terms):
        """ Is is_a(obj,t) true for any t in terms ? """
        ids = self.get_mondo_id(obj.identifier)        
        results = [ i for i in ids for candidate_ancestor in terms if super(Mondo2,self).is_a(i, candidate_ancestor) ] \
                 if terms else []
        return len(results) > 0, results

    def is_genetic_disease(self,obj):
        """Checks mondo to find whether the subject has DOID:630 as an ancestor"""
        return self.has_ancestor(obj, GENETIC_DISEASE)

    def is_monogenic_disease(self,obj):
        """Checks mondo to find whether the subject has DOID:0050177 as an ancestor"""
        return self.has_ancestor(obj, MONOGENIC_DISEASE)

    def doid_get_genetic_condition(self,disease):
        """ This is really rough. Getting ugly. """
        is_genetic, ancestors = self.is_genetic_disease(disease)
        return [
            (self.get_edge({}, 'is_gentic_condition'),
             KNode(ancestor, node_types.GENETIC_CONDITION) ) for ancestor in ancestors
            ] if is_genetic else []

    """ No indication anyone ever calls these three. And the fourth is called internally by something we're replacing. """
    def doid_get_orphanet_genetic_condition (self, disease):
        results = self.doid_get_genetic_condition (disease)
        return [ r for r in results if r[1].identifier.startswith ('ORPHANET.GENETIC_CONDITION') ]
    
    def doid_get_doid_genetic_condition (self, disease):
        results = self.doid_get_genetic_condition (disease)
        return [ r for r in results if r[1].identifier.startswith ('DOID.GENETIC_CONDITION') ]

    def substring_search(self,name):
        ciname = '(?i){}'.format(name)
        results = super(Mondo2,self).search(ciname,synonyms=True,is_regex=True)

    def case_insensitive_search(self,name):
        ciname = '(?i)^{}$'.format(name)
        return super(Mondo2,self).search(ciname,synonyms=True,is_regex=True)

    def search(self, name):
        results = super(Mondo2,self).search(name)
        if len(results) == 0:
            if ',' in name:
                parts =name.split(',')
                parts.reverse()
                ps = [p.strip() for p in parts]
                newname = ' '.join(ps)
                results = super(Mondo2,self).search(newname)
        return results

def test_both():
    q1in='q1-disease-list.txt'
    q1out='q1_disease_mondo.txt'
    q1field = 0
    test_one(q1in, q1out, q1field)
    q2in='q2-drugandcondition-list.txt'
    q2out='q2_disease_mondo.txt'
    q2field = 1
    test_one(q2in, q2out, q2field)
    
def test_one(infname,outfname,fieldnum):
    m = Mondo (ServiceContext.create_context ())
    n_good = 0
    n_bad = 0
    diseases = set()
    with open(infname,'r') as inf, open(outfname,'w') as outf:
        h = inf.readline()
        for line in inf:
            if line.startswith('#'):
                continue
            x = line.strip().split('\t')[fieldnum]
            if x in diseases:
                continue
            diseases.add(x)
            result = m.search (x)
            if len(result) == 0:
                mondos = ''
                names = ''
                doids = ''
                umlss = ''
                efos  = ''
                n_bad += 1
            else:
                n_good += 1
                mondos = ';'.join( result )
                names = ';'.join([ m.get_label( r ) for r in result ])
                doids = ';'.join(sum([ m.mondo_get_doid( r ) for r in result ], [] ))
                umlss = ';'.join(sum([ m.mondo_get_umls( r ) for r in result ], [] ))
                efos = ';'.join(sum([ m.mondo_get_efo( r ) for r in result ], [] ))
            outf.write('{}\t{}\t{}\t{}\t{}\n'.format(x, mondos, doids, umlss, efos ))
            print( 'Good: {}   Bad: {}'.format(n_good, n_bad) )


