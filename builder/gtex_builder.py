from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode, KEdge
from greent.export import BufferedWriter
from greent.util import LoggingUtil
from builder.buildmain import run
from builder.question import LabeledID
from multiprocessing import Pool
from statistics import median
from collections import namedtuple

import logging, time, csv, pickle, gzip

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class GTExBuilder(object):

    def __init__(self, rosetta, debug=False):
        self.rosetta = rosetta
        self.cache = rosetta.cache
        self.clingen = rosetta.core.clingen
        self.gtexcatalog = rosetta.core.gtexcatalog
        self.myvariant = rosetta.core.myvariant
        self.ensembl = rosetta.core.ensembl
        self.concept_model = rosetta.type_graph.concept_model

        return 0

    def create_gtex_graph(self,
                        gtex_nodes,
                        max_hits=100000,
                        reference_genome='HG19',
                        analysis_id=None):
        return 0

if __name__ == '__main__':
    gtb = GTExBuilder(Rosetta(), debug=True)

    # gtb.prepopulate_gwascatalog_cache()

    gtex_directory = '/example_directory'

    gtex_id = ''
    gtex_node = KNode(gtex_id, name='GTEx', type=node_types.ANATOMICAL_ENTITY)

    associated_nodes = []
    associated_file_names = {}
    gtb.create_gtex_graph(associated_nodes, associated_file_names, gtex_directory, analysis_id='testing_gtex')
