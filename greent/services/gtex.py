import requests
from ftplib import FTP
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json
from collections import defaultdict

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class GTEX(Service):
    def __init__(self, context, rosetta):
        super(GTEX, self).__init__("gtex", context)
        self.synonymizer = rosetta.synonymizer
