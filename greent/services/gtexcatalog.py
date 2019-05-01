import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json
from collections import defaultdict

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class GTExCatalog(Service):
    def __init__(self, context, rosetta):
        super(GTExCatalog, self).__init__("gtexcatalog", context)
        self.synonymizer = rosetta.synonymizer


    def prepopulate_cache(self):
        return 0

