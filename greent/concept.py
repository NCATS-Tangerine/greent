import os
from greent.util import Resource
from greent.graph_components import LabeledID
from collections import defaultdict

#TODO: should all of this be done with some sort of canned semantic tools?
class Concept:
    """ A semantic type or concept. A high level idea comprising one or more identifier namespace.
    Provides rudimentary notion of specialization via is_a. """
    def __init__(self, name, is_a, id_prefixes):
        self.name = name
        #Only a single parent?
        self.is_a = is_a
        is_a_name = is_a.name if is_a else None
        self.id_prefixes = [] if id_prefixes is None else id_prefixes
    def __repr__(self):
        return f"Concept(name={self.name},is_a={self.is_a is not None},id_prefixes={self.id_prefixes})"
        #return f"Concept(name={self.name},is_a={self.is_a},id_prefixes={self.id_prefixes})"

class Relationship:
    """ A semantic type for a relationship (or slot)
    Provides rudimentary notion of specialization via is_a. """
    def __init__(self, name, is_a, mappings):
        self.name = name
        #Only a single parent?
        self.is_a = is_a
        is_a_name = is_a.name if is_a else None
        self.mappings = [] if mappings is None else mappings
        self.identifier = self.mint_identifier()
    def mint_identifier(self):
        """Find out what we are going to use as an identifier.
        If we don't have any mappings, mint one based on the name.
        Otherwise, prefer a set of curies from the mappings.  Failing anything else,
        use the first mapping."""
        if len(self.mappings) == 0:
            return f'BIOLINK:{self.name}'
        favorites=['BIOLINK','RO','SIO','BFO','GENO','SEMMEDDB']
        for favorite in favorites:
            for mapped in self.mappings:
                if mapped.startswith(favorite):
                    return mapped
        return self.mappings[0]
    def __repr__(self):
        return f"Relation(name={self.name},is_a={self.is_a is not None},mappings={self.mappings})"
        #return f"Concept(name={self.name},is_a={self.is_a},id_prefixes={self.id_prefixes})"


class ConceptModel:
    """ A grouping of concepts.
    Should ultimately be generalizable to different concept models. We begin with the biolink-model. """
    
    def __init__(self, name):
        self.name = name
        self.by_name = {} #defaultdict(lambda:None)
        self.by_prefix = defaultdict(lambda:None)
        self.relations_by_name = defaultdict(lambda:None)
        self.relations_by_xref = defaultdict(lambda:None)

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

    def add_relationship(self,relationship):
        self.relations_by_name[relationship.name] = relationship
        for mapping in set(relationship.mappings):
            if mapping in self.relations_by_xref:
                raise Exception('Have multiple slots with the same mapping {}'.format(mapping))
            self.relations_by_xref[mapping] = relationship

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

    def get_leaves(self):
        """Return all concepts that are not parents of another concept"""
        leaves = set( self.by_name.values() )
        for concept_name, concept in self.by_name.items():
            if concept.is_a is not None:
                leaves.discard(concept.is_a)
        return leaves

    def get_roots(self):
        """Return all concepts that are parents but do not have parents"""
        parents = set( [concept.is_a for name,concept in self.by_name.items()] )
        return list(filter( lambda x: x is not None and x.is_a is None, parents))

    def standardize_relationship(self,relationship):
        xref = relationship.identifier
        r = self.relations_by_xref[xref]
        if r is None:
            return LabeledID(identifier = "GAMMA:0", label = "Unmapped_Relation")
        else:
            return LabeledID(identifier =r.identifier, label = r.name)

class ConceptModelLoader:

    def __init__(self, name, concept_model):
        self.name = name
        self.model = concept_model
        model_path = os.path.join (os.path.dirname (__file__), "conf", f"{self.name}.yaml")
        model_obj = Resource.load_yaml (model_path)

        model_overlay_path = model_path.replace (".yaml", "_overlay.yaml")
        if os.path.exists (model_overlay_path):
            model_overlay = Resource.load_yaml (model_overlay_path)
            #Update only adds/overwrites keys at the top level. Here, it just overwrites "Classes" rather than updating classes.
            #model_obj.update (model_overlay)
            #This version recursively updates throughout the hierarchy of dicts, updating lists also
            Resource.deepupdate(model_obj, model_overlay)

        for obj in model_obj["classes"]:
            concept = self.parse_item (obj)
            self.model.add_item (concept)

        #THIS is a hack
        self.model.get('gene').id_prefixes = ['HGNC','NCBIGENE','ENSEMBL','MGI','ZFIN']

        for obj in model_obj['slots']:
            relationship = self.parse_slot(obj)
            self.model.add_relationship(relationship)

    def parse_item (self, obj):
        raise ValueError ("Not implemented")

    def parse_slot (self, obj):
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

    def parse_slot(self,obj):
        name = obj["name"].replace (" ", "_")
        mappings = obj["mappings"] if "mappings" in obj else []
        is_a = obj["is_a"].replace (" ", "_") if "is_a" in obj else None
        parent = self.model.relations_by_name [is_a] if is_a in self.model.relations_by_name else None
        return Relationship (name = name, is_a = parent, mappings = mappings)

