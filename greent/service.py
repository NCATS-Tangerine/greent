import os
from reasoner.graph_components import KEdge, KNode
from greent.config import Config
from greent.util import LoggingUtil

class ServiceContext:
    """ A context for all service objects. Gives us a bit of control over how services behave
    and a common point of coniguration. """
    def __init__(self, config=None):
        self.config = Config (config if config else os.path.join (os.path.dirname (__file__), "greent.conf"))
    @staticmethod
    def create_context (config=None):
        return ServiceContext (config)
    
class Service:
    """ Basic characteristics of services. """
    def __init__(self, name, context):
        """ Initialize the service given a name and an application context. """
        self.context = context
        self.name = name
        self.url = context.config.get_service (self.name)["url"]

        setattr (self.context, self.name, self)
        
    def _type(self):
        return self.__class__.__name__
    
    def get_edge (self, props={}, predicate=None, pmids=[]):
        """ Generate graph edges in a standard way, propagating information needed for
        scoring and semantic context above. """
        if not isinstance (props, dict):
            raise ValueError ("Properties must be a dict")

        # Add a predicate describing the connection between subject and object.
        # Pass up pmids for provenance and confidence scoring.
#        print (pmids)
        props['stdprop'] = {
            'predicate' : predicate,
            'pmids'     : pmids
        }
        return KEdge (self.name, predicate, props, is_synonym = (predicate=='synonym'))
