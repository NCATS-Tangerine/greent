import os
from greent.graph_components import KEdge, KNode
from greent.util import LoggingUtil
    
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

    def get_config(self):
        return self.context.config.get_service (self.name)
    
    def get_edge (self, props={}, predicate=None, pmids=[]):
        """ Generate graph edges in a standard way, propagating information needed for
        scoring and semantic context above. """
        if not isinstance (props, dict):
            raise ValueError ("Properties must be a dict")

        # Add a predicate describing the connection between subject and object.
        # Pass up pmids for provenance and confidence scoring.
        props['stdprop'] = {
            'pred'  : predicate,
            'pmids' : pmids
        }
        return KEdge (self.name, predicate, props, is_synonym = (predicate=='synonym'))
