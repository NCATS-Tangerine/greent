import pytest
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.conftest import rosetta
from builder.gtex_builder import GTExBuilder
from builder.obh_builder import get_ordered_names_from_csv

@pytest.fixture()
def gtex(rosetta):
	return GTExBuilder(rosetta, debug=True)

def test_gtex_builder(rosetta, gtex):
    # this will actually write to neo4j
    # create a graph with just one node / file

    gtex_id = ''
    gtex_node = KNode(gtex_id, name='', type=node_types.ANATOMICAL_ENTITY)

    associated_nodes = [gtex_node]
    associated_file_names = {id: 'sample_gtex'}

    gtex_directory = '.'

    gtex.create_gtex_graph(associated_nodes, associated_file_names, gtex_directory, data_set_tag='testing_analysis')
    pass