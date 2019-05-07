import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json
from collections import defaultdict

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

#############
# The GTEX catalog service.
# An interface to the GTEx significant variant/gene expression effects on tissues.
#############
class GTExCatalog(Service):
    ########
    # constructor
    ########
    def __init__(self, context, rosetta):
        super(GTExCatalog, self).__init__("gtexcatalog", context)
        self.synonymizer = rosetta.synonymizer

    ########
    # pre-populate the redis cache database.
    # This should be done if not already accomplished using the builder functionality
    ########
    def prepopulate_gtex_catalog_cache(self):
        return None

    ########
    # define the variant/gene tissue expression relationships
    ########
    def variant_to_gene_expression(self):
        return None
