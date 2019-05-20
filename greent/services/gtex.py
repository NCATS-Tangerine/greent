from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil

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
        #gtb = GTExBuilder(Rosetta())

        # load the redis cache with GTEx data
        #gtb.prepopulate_gtex_catalog_cache()

        # call the GTEx builder to load the cache and graph database
        #gtb.create_gtex_graph(file_path, file_names, 'GTEx service')

        return None

    ########
    # define the variant/gene relationship
    # param: KNode sv_node
    ########
    def sequence_variant_to_gene(self, sv_node):
        # check the input parameters
        if sv_node is None or not isinstance(sv_node, KNode):
            return None

        # declare the return value
        retVal = []

        # create a label identifier

        #

        # return to the caller
        return retVal

    ########
    # Retrieve the variant/anatomy relationship
    #   check for valid input params
    #   convert sequence variant HGVS expression to a GTEx variant id expression
    #   call the GTEx API web service
    #   for each significant variant returned
    #
    # param: KNode sv_node
    ########
    def sequence_variant_to_anatomy(self, sv_node):
        # check the input parameters
        if sv_node is None or not isinstance(sv_node, KNode):
            return None

        return None

    ########
    # define the gene/anatomy relationship
    # param: KNode gene_node
    ########
    def gene_to_anatomy(self, gene_node):
        # check the input parameters
        if gene_node is None or not isinstance(gene_node, KNode):
            return None

        return None
