import os
from greent.graph_components import KEdge
from greent.util import LoggingUtil
from datetime import datetime as dt
import time
    
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
            logger = LoggingUtil.init_logging(__name__)
            logger.warn(f"Unable to get config for service: {self.name}")
            #traceback.print_exc ()
        return result

    def standardize_predicate(self, predicate, source=None, target=None):
        return self.concept_model.standardize_relationship(predicate)

    def create_edge(self, source_node, target_node, provided_by, input_id, predicate, analysis_id=None, publications=[], url=None, properties={}):
        ctime = time.time()
        standard_predicate=self.standardize_predicate(predicate, source_node.id, target_node.id)
        if provided_by is None:
            raise 'missing edge source'
        return KEdge(source_id=source_node.id,
                     target_id=target_node.id,
                     provided_by=provided_by,
                     ctime=ctime,
                     original_predicate=predicate,
                     standard_predicate=standard_predicate,
                     input_id=input_id,
                     publications=publications,
                     url=url,
                     properties=properties)

