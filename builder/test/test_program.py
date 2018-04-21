import pytest
from greent.graph_components import KNode
from greent import node_types
from builder.userquery import UserQuery


@pytest.fixture(scope='module')
def rosetta():
    from greent.rosetta import Rosetta
    return Rosetta()


def test_simple_query(rosetta):
    disease_name = 'test_name'
    did = 'DOID:123'
    disease_identifiers = [did]
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.GENE)
    qd.add_transition(node_types.GENETIC_CONDITION)
    qd.compile_query(rosetta)
    programs = qd.get_programs()
    p = programs[0]
    path = p.get_path_descriptor()
    assert path[0] == (1,1)
    assert path[1] == (2,1)
    assert len(path) == 2

def test_two_sided_query(rosetta):
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_identifiers = ['CTD:123']
    disease_name = 'test_disease'
    disease_name_node = KNode('{}.{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    disease_identifiers = ['DOID:123']
    query = UserQuery(drug_identifiers, node_types.DRUG, drug_name_node)
    query.add_transition(node_types.GENE)
    query.add_transition(node_types.PROCESS)
    query.add_transition(node_types.CELL)
    query.add_transition(node_types.ANATOMY)
    query.add_transition(node_types.PHENOTYPE)
    query.add_transition(node_types.DISEASE, end_values=disease_identifiers)
    query.add_end_lookup_node(disease_name_node)
    query.compile_query(rosetta)
    programs = query.get_programs()
    p = programs[0]
    path = p.get_path_descriptor()
    assert path[0] == (1,1)
    assert path[1] == (2,1)
    assert path[2] == (3,1)
    assert path[3] == (4,1)
    assert path[4] == (5,-1)
    assert path[5] == (6,-1)
    assert len(path) == 6
