import pytest
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.conftest import Rosetta
from builder.gtex_builder import GTExBuilder

@pytest.fixture()
def gtb(rosetta):
	return GTExBuilder(rosetta, debug=True)

"""
def test_gtex_builder(rosetta, gtb):
    # this will actually write to neo4j
    # create a graph with just one node / file

    # load the redis cache
    gtb.prepopulate_gtex_catalog_cache()

    # directory with GTEx data to process
    gtex_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'

    # create a node
    gtex_id = '0002190'  # ex. uberon "Adipose Subcutaneous"
    gtex_node = KNode(gtex_id, name='gtex_tissue', type=node_types.ANATOMICAL_ENTITY)

    # assign the node to an array
    associated_nodes = [gtex_node]

    # assign the name of the GTEx data file
    associated_file_names = {'little_signif.csv'}

    # call the GTEx builder
    gtb.create_gtex_graph(associated_nodes, associated_file_names, gtex_directory, 'testing_gtex')
    pass
"""

def test_gtex_cache_loaded(rosetta, gtb):
   cached = rosetta.cache.get('myvariant.sequence_variant_to_gene(CAID:CA248392703)')
   assert(cached)

