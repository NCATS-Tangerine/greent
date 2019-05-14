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
class GTEx(Service):
    ########
    # constructor
    ########
    def __init__(self, context, rosetta):
        super(GTEx, self).__init__("gtex", context)
        self.synonymizer = rosetta.synonymizer

    ########
    # define the manual way to preload redis cache
    ########
    def prepopulate_gtex_catalog_cache(self):
        return None

    ########
    # define the variant/gene relationship
    ########
    def sequence_variant_to_gene(self):
        return None

    ########
    # define the variant/anatomy relationship
    ########
    def sequence_variant_to_anatomy(self):
        return None

    ########
    # define the gene/anatomy relationship
    ########
    def gene_to_anatomy(self):
        return None
