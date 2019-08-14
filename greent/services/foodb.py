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
        self.foodList = None
        super(FooDB, self).__init__("foodb", context)

    # gets a list of fooDB food records
    def loadAllFoods(self):
        self.foodList = list
        return None

    def getContentsRecord(self, foodId):
        return None

    def getCompoundsRecord(self, foodId):
        return None

    # food to chemical data crawling interface
    def food_to_chemical_compound(self, food):
        return None

    # chemical to food data crawling interface
    def chemical_compound_to_food(self, chemical):
        return None

