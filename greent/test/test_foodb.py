import pytest
from greent.services.foodb import FooDB
from greent.servicecontext import ServiceContext
from greent.graph_components import KNode
from greent import node_types
from greent.graph_components import LabeledID

@pytest.fixture()
def foodb():
    foodb = FooDB(ServiceContext.create_context())
    return foodb

def test_load_foods(foodb: ServiceContext):
    # load up the foods.csv file
    results = foodb.load_all_foods('C:/Phil/Work/Informatics/Robokop/FooDB/FooDB_rawdata/foods.csv')

    assert(results)

def test_food_to_chemical_substance(foodb: ServiceContext):
    # load up the foods.csv file
    results = foodb.load_all_foods('C:/Phil/Work/Informatics/Robokop/FooDB/FooDB_rawdata/foods.csv')

    # create a food node
    food_node = KNode(results[0], type=node_types.FOOD)

    # call to get a chemical substance node and a edge label
    chem_subst = foodb.food_to_chemical_substance(food_node)

    assert(chem_subst)