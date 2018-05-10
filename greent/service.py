import os
from greent.graph_components import KEdge, KNode
from greent.util import LoggingUtil
from datetime import datetime as dt
    
class Service:
    """ Basic characteristics of services. """
    def __init__(self, name, context):
        """ Initialize the service given a name and an application context. """
        self.context = context
        self.name = name
        self.url = context.config.get_service (self.name).get("url", None)
        try:
            self.concept_model = getattr(context, 'rosetta-graph').concept_model
        except:
            pass
        setattr (self.context, self.name, self)

    def _type(self):
        return self.__class__.__name__

    def get_config(self):
        result = {}
        try:
            result = self.context.config.get_service (self.name)
        except:
            logger = LoggingUtil.init_logging(__file__)
            logger.warn(f"Unable to get config for service: {self.name}")
            #traceback.print_exc ()
        return result

    def standardize_predicate(self, predicate):
        return self.concept_model.standardize_relationship(predicate)

    def create_edge(self,source_node,target_node,edge_source,input_id,predicate,publications=None,url=None,properties=None):
        ctime = dt.now()
        standard_predicate=self.standardize_predicate(predicate)
        return KEdge(source_node,
                     target_node,
                     edge_source,
                     ctime,
                     predicate,
                     standard_predicate,
                     input_id,
                     publications,
                     url,
                     properties)

