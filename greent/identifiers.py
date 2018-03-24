import os
from collections import defaultdict
from greent.util import Resource

class Identifiers:
    """ Model relationships betwen identifiers and curies. """
    def __init__(self):
        self.vocab = defaultdict(lambda:None)
        self.curie_to_identifier = defaultdict(lambda:None)
        self.identifier_to_curie = defaultdict(lambda:None)
        
        uber = None
        try:
            uber = Resource.get_resource_obj(os.path.join(os.path.dirname (__file__), "conf", "uber_context.jsonld"))
        except:
            uber['@context'] = {}

        """ A convenient repository of curie mappings. """
        context = uber['@context']
        self.terminate(context)
        for k, v in context.items():
            self.curie_to_identifier[k] = v
            if isinstance(v, str):
                self.identifier_to_curie[v] = k
                self.vocab[k] = v

        path = os.path.join(os.path.dirname (__file__), "conf", "identifiers.org.json")
        """ Identifiers.org data set. """
        self.identifiers_org = Resource.get_resource_obj(path)
        for module in self.identifiers_org:
            curie = module['prefix'].upper()
            url = module['url']
            self.curie_to_identifier[curie] = url
            self.identifier_to_curie[url] = curie

        """ These identifiers are used in the translator registry but not loaded from previous sources. """
        names = {
            "HGNC"             : [
                "http://identifiers.org/hgnc/"
            ],
            "HP"             : [
                "http://identifiers.org/hp/"
            ],
            "NAME.DRUG"      : [
                "http://identifiers.org/drugname/"
            ],
            "NAME.DISEASE"   : [
                "http://biothings.io/concepts/disease_name/"
            ],
            "NAME.SYMPTOM"   : [],
            "NAME.ANATOMY"   : [],
            "NAME.PHENOTYPE" : []
        }
        for k, v in names.items ():
            for vv in v:
                self.curie_to_identifier[k] = vv            
                self.identifier_to_curie[vv] = k
            
    def id2curie (self, identifier):
        """ Convert an IRI namespace identifier to a curie. """
        return self.identifier_to_curie[identifier]

    def curie2id (self, curie):
        """ Convert a curie to an identifier. """
        return self.curie_to_identifier[curie]

    def curie_instance2id (self, curie_instance):
        """ Convert an object instance with a curie prefix to an IRI prefixed instance id. """
        curie, i = curie_instance.split (":")
        return f"{self.curie2id(curie)}/{i}"
                                    
    def instance2curie (self, obj):
        """ Given an instance prefixed by an IRI, return an instance prefixed by a curie. """
        result = None
        if not obj.startswith ("http://") and ":" in obj:
            result = obj
        elif '/' in obj:
            parts = obj.split ("/")
            iri = "/".join (parts[:-1])
            key = parts[-1]
            curie = self.id2curie (iri)
            result = f"{curie}:{key}"
        return result
    
    def terminate(self, d):
        for k, v in d.items():
            if isinstance(v, str) and not v.endswith("/"):
                d[k] = "{0}/".format(v)

