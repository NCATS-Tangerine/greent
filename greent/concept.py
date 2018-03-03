
class Concept:

    def __init__(self, name, is_a, id_prefixes):
        self.name = name
        self.is_a = is_a
        is_a_name = is_a.name if is_a else None
        self.id_prefixes = [] if id_prefixes is None else id_prefixes
        print ("name {} is_a {} prefixes {}".format (name, is_a_name, id_prefixes))
        
class ConceptModel:
    
    def __init__(self, name):
        self.name = name
        self.by_name = defaultdict(Concept)
        
    def add_item (self, obj):
        name = obj["name"].replace (" ", "_")
        is_a = obj["is_a"].replace (" ", "_") if "is_a" in obj else None
        id_prefixes = obj["id_prefixes"] if "id_prefixes" in obj else None
        parent = self.by_name [is_a] if is_a in self.by_name else None
        self.by_name [name] = Concept (name = name,
                                       is_a = parent,
                                       id_prefixes = id_prefixes)

    def items (self):
        return  self.by_name.items ()
    
