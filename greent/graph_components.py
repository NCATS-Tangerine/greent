from collections import defaultdict
from functools import singledispatch
from greent.node_types import node_types
from greent.util import Text
from json import JSONEncoder
from json import JSONDecoder

class GenericJSONEncoder(JSONEncoder):
    def default(self, o):
        d = { '__class__':obj.__class__.__name__, 
              '__module__':obj.__module__,
        }
        d.update(obj.__dict__)
        return d
class KNodeEncoder(GenericJSONEncoder):
    pass
class KNodeEncoder(GenericJSONEncoder):
    pass
class GenericJSONDecoder(JSONDecoder):
    def __init__(self, encoding=None):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)
    def dict_to_object(self, d):
        if '__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module = __import__(module_name)
            class_ = getattr(module, class_name)
            args = dict( (key.encode('ascii'), value) for key, value in d.items())
            inst = class_(**args)
        else:
            inst = d
        return inst
class KNodeDecoder(GenericJSONDecoder):
    pass
class KEdgeEncoder(GenericJSONEncoder):
    pass


class KNode():
    """Used as the node object in KnowledgeGraph.
    
    Instances of this class can be passed to WorldGraph/greent as query subjects/objects."""

    def __init__(self, identifier, node_type, label=None):
        if identifier.startswith('http'):
            identifier = Text.obo_to_curie(identifier)
        self.identifier = identifier
        self.label = label
#        if node_type not in node_types:
#            raise ValueError('node_type {} unsupported.'.format(node_type))
        self.node_type = node_type
        self.properties = {}
        self.mesh_identifiers = []
        self.synonyms = set()
        self.synonyms.add(identifier)
        self.contexts = defaultdict(set)

    def add_synonyms(self, new_synonym_set):
        self.synonyms.update(new_synonym_set)

    def get_synonyms_by_prefix(self, prefix):
        return set( filter(lambda x: Text.get_curie(x) == prefix, self.synonyms) )

    def add_context(self, program_id, context):
        self.contexts[program_id].add(context)

    def get_context(self,program_id):
        return self.contexts[program_id]

    def add_synonym(self, synonymous_node):
        """Merge anther KNode (representing a synonym) into this KNode."""
        self.synonyms.add(synonymous_node.identifier)
        for prog_id in synonymous_node.contexts:
            self.contexts[prog_id].update(synonymous_node.contexts[prog_id])
        for propkey in synonymous_node.properties:
            if propkey in self.properties:
                # TODO: this is messy
                if type(self.properties[propkey]) != type(synonymous_node.properties[propkey]):
                    raise Exception('Problem merging properties {}, {}'.format(type(self.properties[propkey]), type(
                        synonymous_node.properties[propkey])))
                if isinstance(self.properties[propkey], list):
                    self.properties[propkey] += synonymous_node.properties[propkey]
                elif isinstance(self.properties[propkey], set):
                    self.properties[propkey].update(synonymous_node.properties[propkey])
                else:
                    self.properties[propkey] = [self.properties[propkey], synonymous_node.properties[propkey]]
            else:
                self.properties[propkey] = synonymous_node.properties[propkey]

    def __repr__(self):
        # return "KNode(id={0},type={1})".format (self.identifier, self.node_type)
        return "N({0},t={1})".format(self.identifier, self.node_type)

    def __str__(self):
        return self.__repr__()

    # Is using identifier sufficient?  Probably need to be a bit smarter.
    def __hash__(self):
        """Class needs __hash__ in order to be used as a node in networkx"""
        return self.identifier.__hash__()

    def __eq__(x, y):
        return x.identifier == y.identifier

    def to_json(self):
        """Used to serialize a node to JSON."""
        j = {'identifier': self.identifier, \
             'node_type': self.node_type}
        '''
        if self.layer_number is not None:
            j['layer_number'] = self.layer_number
        '''
        for key in self.properties:
            j[key] = self.properties[key]
        self.properties['id'] = id(self)
        return j

    def get_shortname(self):
        """Return a short user-readable string suitable for display in a list"""
        if self.label is not None:
            return '%s (%s)' % (self.label, self.identifier)
        return self.identifier

class KEdge():
    """Used as the edge object in KnowledgeGraph.

    Instances of this class should be returned from greenT"""

    def __init__(self, edge_source, ctime, predicate_id, predicate_label, input_id, standard_predicate_id, standard_predicate_label, publications = None, url=None, properties=None, is_support=False):
        """Definitions of the parameters:
        edge_function: the python function called to produce this edge
        ctime: When the external call to produce this edge was made.  If the edge comes from a cache, this
               will be the original time, not the cache retrieval time.
        predicate_id: The identifier for the predicate as returned from the knowledge source
        predicate_label: The label for the predicate as returned from the knowledge source
        input_id: The actual identifier used as input to the knowledge service
        standard_predicate_id: The identifier for the predicate as converted into a shared standard (e.g. biolink)
        standard_predicate_label: The label for the predicate as converted into a shared standard (e.g. biolink)
        publications: a list of pubmed id curies that provide evidence for or create this edge.
        url: the url that actually created the edge (for calls that can be so coded.). Optional
        properties: A map of any other information about the edge that we may want to persist.  Default None.
        is_support: Whether or not the edge is a support edge. Default False.
        """
        self.edge_source = edge_source
        self.source_node = None
        self.target_node = None
        self.ctime = ctime
        self.predicate_id = predicate_id
        self.predicate_label = predicate_label
        self.input_id = input_id
        self.standard_predicate_id = standard_predicate_id
        self.standard_predicate_label = standard_predicate_label
        self.publications = publications
        validate(self.publications)
        if properties is not None:
            self.properties = properties
        else:
            self.properties = {}
        self.is_support = is_support

    def __key(self):
        return (self.source_node, self.target_node, self.edge_source)

    def __eq__(x, y):
        return x.__key() == y.__key()

    def __hash__(self):
        return hash(self.__key())

    def long_form(self):
        return "E(src={0},type={1},srcn={2},destn={3})".format(self.edge_source, self.edge_function,
                                                               self.source_node, self.target_node)


    def to_json(self):
        """Used to serialize a node to JSON."""
        j = {'edge_source': self.edge_source, \
             'edge_function': self.edge_function, \
             'is_synonym': self.is_synonym}
        for key in self.properties:
            j[key] = self.properties[key]
        return j

    def __repr__(self):
        # return "KEdge(edge_source={0},edge_type={1})".format (self.edge_source, self.edge_type)
        return self.long_form()

    #        return "E(src={0},type={1})".format (self.edge_source, self.edge_type)
    def __str__(self):
        return self.__repr__()

# We want to be able to serialize our knowledge graph to json.  That means being able to serialize KNode/KEdge.
# We could sublcass JSONEncoder (and still might), but for now, this set of functions allows the default
# encoder to find the functions that return serializable versions of KNode and KEdge
##

@singledispatch
def elements_to_json(x):
    """Used by default in dumping JSON. For use by json.dump; should not usually be called by users."""
    # The singledispatch decorator allows us to register serializers in our edge and node classes.
    return str(x)


@elements_to_json.register(KNode)
def node_to_json(knode):
    """Routes JSON serialization requests to KNode member function.  Not for external use."""
    return knode.to_json()


@elements_to_json.register(KEdge)
def node_to_json(kedge):
    """Routes JSON serialization requests to KEdge member function.  Not for external use."""
    return kedge.to_json()

# END JSON STUFF
