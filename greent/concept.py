import json
import os
from greent.util import Resource
from collections import defaultdict

class Concept:
    """ A semantic type or concept. A high level idea comprising one or more identifier namespace.
    Provides rudimentary notion of specialization via is_a. """
    def __init__(self, name, is_a, id_prefixes):
        self.name = name
        self.is_a = is_a
        is_a_name = is_a.name if is_a else None
        self.id_prefixes = [] if id_prefixes is None else id_prefixes
    def __repr__(self):
        return f"Concept(name={self.name},is_a={self.is_a is not None},id_prefixes={self.id_prefixes})"
class ConceptModel:
    """ A grouping of concepts.
    Should ultimately be generalizable to different concept models. We begin with the biolink-model. """
    
    def __init__(self, name):
        self.name = name
        self.by_name = {} #defaultdict(lambda:None)
        self.by_prefix = defaultdict(lambda:None)

        self.model_loaders = {
            'biolink-model' : lambda : BiolinkConceptModelLoader (name, self)
        }
        if self.name in self.model_loaders:
            self.model_loaders[self.name] ()
        else:
            raise ValueError (f"A concept model loader for concept model {self.name} must be defined.")
        
        """ Design discussions are ongoing about how best to model concepts with respect to ids. Manual annotation
        provides the most reliable and granular approach but is time intensive. It's unclear when this standard might
        be met. Another approach is to reason about concepts based on identifiers. This overloads the semantics of 
        identifiers which is concerning. For now, we plan to look first in the model. If a curator has made an assertion
        about an association between the model and an identifier, we prioritize that. If not, we guess based on a map
        of associations between identifiers and concepts. """
        identifier_map_path = os.path.join (os.path.dirname (__file__), "conf", "identifier_map.yaml")
        self.the_map = Resource.load_yaml (identifier_map_path)['identifier_map']
        #for c in self.by_name.values ():
        #    print (f"by name {c}")
    def get (self, concept_name):
        return self.by_name[concept_name]
    
    def add_item (self, concept):
        self.by_name [concept.name] = concept
        for prefix in concept.id_prefixes:
            self.by_prefix[prefix] = concept

    def items (self):
        return  self.by_name.items ()

    def get_concepts_by_prefix (self, id_list):
        return [ self.by_prefix[k].name for k in id_list if k in self.by_prefix ]

    def get_single_concept_by_prefixes (self, id_list):
        possible_concepts = defaultdict(lambda: [None, 0])
        for k in id_list:
            if k in self.by_prefix:
                node = possible_concepts[self.by_prefix[k]]
                node[0] = k
                node[1] = node[1] + 1
        ordered = sorted(possible_concepts.items (), key=lambda item: item[1])
        return ordered[0][0] if len(ordered) > 0 else None

class ConceptModelLoader:

    def __init__(self, name, concept_model):
        self.name = name
        self.model = concept_model
        model_path = os.path.join (os.path.dirname (__file__), "conf", f"{self.name}.yaml")
        model_obj = Resource.load_yaml (model_path)
        for obj in model_obj["classes"]:
            concept = self.parse_item (obj)
            self.model.add_item (concept)

    def parse_item (self, obj):
        raise ValueError ("Not implemented")

class BiolinkConceptModelLoader (ConceptModelLoader):
    def __init__(self, name, concept_model):
        super(BiolinkConceptModelLoader, self).__init__(name, concept_model)
                
    def parse_item (self, obj):
        name = obj["name"].replace (" ", "_")
        is_a = obj["is_a"].replace (" ", "_") if "is_a" in obj else None
        id_prefixes = obj["id_prefixes"] if "id_prefixes" in obj else []
        parent = self.model.by_name [is_a] if is_a in self.model.by_name else None
        return Concept (name = name, is_a = parent, id_prefixes = id_prefixes)


    
