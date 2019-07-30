import logging
import requests
from datetime import datetime as dt
from greent.service import Service
from greent.graph_components import KNode, LabeledID
from greent.util import Text, LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

class FooDB(Service):
    def __init__(self, context):
        super(FooDB, self).__init__("foodb", context)

    def get_foods(self):
        return None

    # food to chemical data crawling interface
    def food_to_chemical(self, food):
        return None

    # chemical to food data crawling interface
    def chemical_to_food(self, chemcal):
        return None

