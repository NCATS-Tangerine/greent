from collections import defaultdict
from functools import singledispatch
from greent import node_types
from greent.util import Text, LoggingUtil
from typing import NamedTuple
from builder.question import LabeledID
from builder.util import FromDictMixin
import logging

logger = LoggingUtil.init_logging (__name__, level=logging.DEBUG)

class KNode(FromDictMixin):
    """Used as the node object in KnowledgeGraph.
    
    Instances of this class can be passed to WorldGraph/greent as query subjects/objects."""

    def __init__(self, *args, **kwargs):
        self.id = None
        self.name = None
        self.type = None
        self.properties = {}

        if args and len(args) == 1 and isinstance(args[0], str):
            self.id = args[0]
            args = []
        # TODO: Currently hack to only utilize the 1st curie in a list if multiple curies provided
        elif args and len(args) == 1 and isinstance(args[0], list) and isinstance(args[0][0], str):
            self.id = args[0][0]
            args = []

        super().__init__(*args, **kwargs)

        # Another hack to keep things running.
        if isinstance(self.name, list):
            self.name = self.name[0]

        if self.id.startswith('http'):
            self.id = Text.obo_to_curie(self.id)

        #Synonyms is just for CURIEs
        self.synonyms = set()
        self.synonyms.add(LabeledID(identifier=self.id, label=self.name))

    def add_synonyms(self, new_synonym_set):
        """Accepts a collection of either String CURIES or LabeledIDs"""
        #Once I am sure that we're only sending in strings, we can dunk this and go back to just using update
        #self.synonyms.update(new_synonym_set)
        for newsyn in filter(lambda x : x != None, new_synonym_set):
            if isinstance(newsyn,str):
                self.synonyms.add(LabeledID(identifier=newsyn, label=""))
            else:
                #Better be a LabeledID
                self.synonyms.add(newsyn)

    def get_synonyms_by_prefix(self, prefix):
        """Returns curies (not labeledIDs) for any synonym with the input prefix"""
        return set( filter(lambda x: Text.get_curie(x).upper() == prefix.upper(), [s.identifier for s in self.synonyms]) )

    def get_labeled_ids_by_prefix(self, prefix):
        """Returns labeledIDs for any synonym with the input prefix"""
        return set( filter(lambda x: Text.get_curie(x.identifier).upper() == prefix.upper(), self.synonyms) )

    def __repr__(self):
        # return "KNode(id={0},type={1})".format (self.id, self.type)
        return "N({0},t={1})".format(self.id, self.type)

    def __str__(self):
        return self.__repr__()

    # Is using identifier sufficient?  Probably need to be a bit smarter.
    def __hash__(self):
        """Class needs __hash__ in order to be used as a node in networkx"""
        return self.id.__hash__()

    def __eq__(self, other):
        if isinstance(self, int) or isinstance(other, int):
            return False
        return self.id == other.id

    def get_shortname(self):
        """Return a short user-readable string suitable for display in a list"""
        if self.name is not None:
            return '%s (%s)' % (self.name, self.id)
        return self.id

    def n2json (self):
        """ Serialize a node as json. """
        return {
            "id"   : self.id,
            "type" : f"blm:{self.type}",
        }

class KEdge(FromDictMixin):
    """Used as the edge object in KnowledgeGraph.

    Instances of this class should be returned from greenT"""

    # def __init__(self, source_node, target_node, provided_by, ctime, original_predicate, standard_predicate, input_id, publications = None, url=None, properties=None, is_support=False):
    def __init__(self, *args, **kwargs):
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
        self.source_id = None
        self.target_id = None
        self.provided_by = None
        self.ctime = None
        self.hyper_edge_id = None
        self.original_predicate = None
        self.standard_predicate = None
        self.input_id = None
        self.publications = []
        self.url = None
        self.is_support = False
        self.properties = {}

        super().__init__(*args, **kwargs)

        if self.provided_by is None:
            raise 'Invalid source?'

        self.validate_publications()

    def load_attribute(self, key, value):
        if key == 'original_predicate' or key == 'standard_predicate':
            return LabeledID(**value) if isinstance(value, dict) else value
        else:
            return super().load_attribute(key, value)

    def __key(self):
        return (self.source_id, self.target_id, self.provided_by, self.original_predicate)

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __lt__(self, other):
        return True

    def __gt__(self, other):
        return False
    
    def __hash__(self):
        return hash(self.__key())

    def long_form(self):
        return "E(src={0},subjn={1},objn={2})".format(self.provided_by, self.source_id, self.target_id)

    def validate_publications(self):
        if self.publications is None:
            self.publications = []
        for publication in self.publications:
            if not isinstance(publication,str):
                raise Exception(f"Publication should be a PMID curie: {publication}")
            if not publication.startswith('PMID:'):
                raise Exception(f"Publication should be a PMID curie: {publication}")
            try:
                int(publication[5:])
            except:
                raise Exception(f"Publication should be a PMID curie: {publication}")

    def __repr__(self):
        return self.long_form()

    def __str__(self):
        return self.__repr__()

    def e2json(self):
        """ Serialize an edge as json. """
        return {
            "ctime"  : str(self.ctime),
            "sub"    : self.source_id,
            "pred"   : self.standard_predicate,
            "obj"    : self.target_id,
            "pubs"   : str(self.publications)
        }
    

