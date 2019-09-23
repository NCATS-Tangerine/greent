import pytest
from greent.services.foodb import FooDB
from greent.servicecontext import ServiceContext
from greent.graph_components import KNode
from greent import node_types


@pytest.fixture()
def foodb(rosetta):
    foodb = rosetta.core.foodb
    return foodb


def test_load_foods(foodb: FooDB):
    # load up the foods.csv file
    results = foodb.load_all_foods('C:/Phil/Work/Informatics/Robokop/FooDB/FooDB_rawdata/foods.csv')

    assert results


def test_food_to_chemical_substance(foodb: FooDB):
    # load up the foods.csv file
    food_list = foodb.load_all_foods('C:/Phil/Work/Informatics/Robokop/FooDB/FooDB_rawdata/foods.csv')

    # create a food node
    food_node = KNode(id=food_list[1], type=node_types.FOOD)

    # call to create and populate the chemical substance nodes/edges
    cs_nodes_and_edges = foodb.food_to_chemical_substance(food_node)

    # TODO: synonymize the chemical substance node

    assert cs_nodes_and_edges
