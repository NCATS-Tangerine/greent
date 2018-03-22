from greent.service import Service
from greent.service import ServiceContext
from ontobio.ontol_factory import OntologyFactory
from greent.graph_components import KNode, KEdge
from greent.util import LoggingUtil
from greent import node_types


logger = LoggingUtil.init_logging (__file__)

#TODO: LOOKUP all the terms that map to this... or use an ancestor call that doesn't require such stuff (i.e. that handles this)
GENETIC_DISEASE=('DOID:630','http://purl.obolibrary.org/obo/EFO_0000508','MONDO:0003847','http://purl.obolibrary.org/obo/MONDO_0003847')
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
            logger.warn('Problem reaching sparql endpoint, falling back to obo')
            try:
                self.ont = ofactory.create('obo:mondo')
            except:
                logger.warn('Problem reaching obo, falling back to local owl file')
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


    def mondo_get_doid(self,mondo_identifier):
        return self.mondo_get_synonym(mondo_identifier,'DOID')

    def mondo_get_umls(self,mondo_identifier):
        return self.mondo_get_synonym(mondo_identifier,'UMLS')

    def mondo_get_efo(self,mondo_identifier):
        return self.mondo_get_synonym(mondo_identifier,'EFO')

    def mondo_get_synonym(self,mondo_identifier,curie_prefix):
        xref_ids = self.ont.xrefs(mondo_identifier)
        doids = []
        for xref_id in xref_ids:
            if xref_id.startswith(curie_prefix):
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


