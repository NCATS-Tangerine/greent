from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
from greent.rosetta import Rosetta

from builder.gtex_builder import GTExBuilder

import requests
import json

# declare a logger...
import logging
# ... and initialize it
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
    # define the manual way to launch processing an input data file
    ########
    def create_gtex_graph(self, file_path, file_names):

        # check the inputs
        if file_path is None or file_names is None:
            logger.error('Error: Missing or invalid input arguments')

        # create a new builder object
        gtb = GTExBuilder(Rosetta())

        # load the redis cache with GTEx data
        gtb.prepopulate_gtex_catalog_cache()

        # call the GTEx builder to load the cache and graph database
        gtb.create_gtex_graph(file_path, file_names, 'GTEx service')

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
