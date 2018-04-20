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
    print( qd.generate_cypher() )
    assert qd.compile_query(rosetta)
    cyphers = qd.generate_cypher()
    assert len(cyphers) == 1
    start_nodes = qd.get_start_node()
    assert len(start_nodes) == 1
    assert start_nodes[0][0] == did
    lookups = qd.get_lookups()
    assert len(lookups) == 1
    assert lookups[0].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    ntypes = qd.get_neighbor_types(node_types.GENE)
    assert len(ntypes) == 1
    neighbors = list(ntypes)[0]
    assert node_types.DISEASE in neighbors
    assert node_types.GENETIC_CONDITION in neighbors

def test_simple_query_with_unspecified(rosetta):
    disease_name = 'test_name'
    did = 'DOID:123'
    disease_identifiers = [did]
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.UNSPECIFIED)
    qd.add_transition(node_types.GENETIC_CONDITION)
    assert qd.compile_query(rosetta)
    cyphers = qd.generate_cypher()
    assert len(cyphers) == 1
    start_nodes = qd.get_start_node()
    assert len(start_nodes) == 1
    assert start_nodes[0][0] == did
    lookups = qd.get_lookups()
    assert len(lookups) == 1
    assert lookups[0].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    reverse = qd.get_reversed()
    assert len(reverse) == 1
    assert not reverse[0]
    #Currently, an unspecified node doesn't get "filled in" so it's hard to do this with.
    ntypes = qd.get_neighbor_types(node_types.GENE)
    assert len(ntypes) == 0
    #assert len(ntypes) == 1
    #neighbors = list(ntypes)[0]
    #assert node_types.DISEASE in neighbors
    #assert node_types.GENETIC_CONDITION in neighbors

def test_simple_query_with_unspecified_at_end(rosetta):
    disease_name = 'test_name'
    did = 'DOID:123'
    disease_identifiers = [did]
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.GENE)
    qd.add_transition(node_types.UNSPECIFIED)
    assert qd.compile_query(rosetta)
    cyphers = qd.generate_cypher()
    #assert len(cyphers) == 1
    start_nodes = qd.get_start_node()
    #assert len(start_nodes) == 1
    assert start_nodes[0][0] == did
    lookups = qd.get_lookups()
    #assert len(lookups) == 1
    assert lookups[0].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    reverse = qd.get_reversed()
    #assert len(reverse) == 1
    assert not reverse[0]
    ntypes = qd.get_neighbor_types(node_types.GENE)
    assert len(ntypes) == 1
    neighbors = list(ntypes)[0]
    assert node_types.DISEASE in neighbors
    #UNSPECIFIED DOESN"T GET FILLED IN - it stays unspecified.
    #assert node_types.GENETIC_CONDITION in neighbors



def test_failing_query(rosetta):
    """IN the current set of edges, there is no gene->anatomy service. If we add one this teset will fail"""
    disease_name = 'test_name'
    did = 'DOID:123'
    disease_identifiers = [did]
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.GENE)
    qd.add_transition(node_types.ANATOMY)
    assert not qd.compile_query(rosetta)


def test_query_set_same_id_type(rosetta):
    disease_name = 'test_name'
    disease_identifiers = ['DOID:123', 'DOID:456']
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.GENE)
    qd.add_transition(node_types.GENETIC_CONDITION)
    assert qd.compile_query(rosetta)
    cyphers = qd.generate_cypher()
    assert len(cyphers) == 2
    start_nodes = qd.get_start_node()
    assert len(start_nodes) == 2
    assert start_nodes[0][0] == disease_identifiers[0]
    assert start_nodes[1][0] == disease_identifiers[1]
    lookups = qd.get_lookups()
    assert len(lookups) == 2
    assert lookups[0].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    assert lookups[1].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    reverse = qd.get_reversed()
    assert len(reverse) == 2
    assert not reverse[0]
    assert not reverse[1]


def test_query_set_different_valid_ids(rosetta):
    disease_name = 'test_name'
    disease_identifiers = ['DOID:123', 'EFO:456']
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.GENE)
    qd.add_transition(node_types.GENETIC_CONDITION)
    assert qd.compile_query(rosetta)
    cyphers = qd.generate_cypher()
    assert len(cyphers) == 2
    start_nodes = qd.get_start_node()
    assert len(start_nodes) == 2
    assert start_nodes[0][0] == disease_identifiers[0]
    assert start_nodes[1][0] == disease_identifiers[1]
    lookups = qd.get_lookups()
    assert len(lookups) == 2
    assert lookups[0].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    assert lookups[1].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    reverse = qd.get_reversed()
    assert len(reverse) == 2
    assert not reverse[0]
    assert not reverse[1]


def test_query_set_different_one_valid_ids(rosetta):
    disease_name = 'test_name'
    disease_identifiers = ['DOID:123', 'FAKEYFAKEY:456']
    name_node = KNode('{}:{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    qd = UserQuery(disease_identifiers, node_types.DISEASE, name_node)
    qd.add_transition(node_types.GENE)
    qd.add_transition(node_types.GENETIC_CONDITION)
    assert qd.compile_query(rosetta)
    #generate_cypher is no longer relevant
    #cyphers = qd.generate_cypher()
    #assert len(cyphers) == 1
    start_nodes = qd.get_start_node()
    assert len(start_nodes) == 1
    assert start_nodes[0][0] == disease_identifiers[0]
    lookups = qd.get_lookups()
    assert len(lookups) == 1
    assert lookups[0].identifier == '{}:{}'.format(node_types.DISEASE_NAME, disease_name)
    reverse = qd.get_reversed()
    assert len(reverse) == 1
    assert not reverse[0]





def test_generate_set(rosetta):
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
    d = query.definition
    l, r = d.generate_paired_query(4)[0]
    assert len(l.transitions) == 4
    assert len(r.transitions) == 2


def test_query_two_sided_queryset(rosetta):
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
    assert query.compile_query(rosetta)


def test_query_two(rosetta):
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_identifiers = ['CTD:Adapalene', 'PHAROS.DRUG:95769', 'PUBCHEM:60164']
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
    assert query.compile_query(rosetta)

def test_split(rosetta):
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_ids = ['CTD:Lisinopril', 'PHAROS.DRUG:128029', 'PUBCHEM:5362119']
    disease_name = 'test_disease'
    disease_name_node = KNode('{}.{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    disease_ids = ['DOID:4325']
    query = UserQuery(drug_ids, node_types.DRUG, drug_name_node)
    query.add_transition(node_types.DISEASE, min_path_length=1, max_path_length=2, end_values=disease_ids)
    definition = query.definition
    splits = definition.generate_paired_query_splitting_transition(0)
    assert len(splits)== 1
    l,r = splits[0]
    print (','.join(l.node_types))
    print (','.join(r.node_types))

def test_d_unknown_d(rosetta):
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_ids = ['CTD:Lisinopril', 'PHAROS.DRUG:128029', 'PUBCHEM:5362119']
    disease_name = 'test_disease'
    disease_name_node = KNode('{}.{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    disease_ids = ['DOID:4325']
    query = UserQuery(drug_ids, node_types.DRUG, drug_name_node)
    query.add_transition(node_types.DISEASE, min_path_length=1, max_path_length=2, end_values=disease_ids)
    query.add_end_lookup_node(disease_name_node)
    assert query.compile_query(rosetta)

def test_d_unknown_p(rosetta):
    """currently we lack sources that would allow this to succeed.  If this test starts passing it is because there is
    now a path going (drug)->(something)<-(phenotype)"""
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_ids = ['CTD:Lisinopril', 'PHAROS.DRUG:128029', 'PUBCHEM:5362119']
    phenotype_name = 'test_phenotype'
    phenotype_name_node = KNode('{}.{}'.format(node_types.PHENOTYPE_NAME, phenotype_name), node_types.PHENOTYPE_NAME)
    phenotype_ids = ['HP:4325']
    query = UserQuery(drug_ids, node_types.DRUG, drug_name_node)
    query.add_transition(node_types.PHENOTYPE, min_path_length=1, max_path_length=2, end_values=phenotype_ids)
    query.add_end_lookup_node(phenotype_name_node)
    assert not query.compile_query(rosetta)


def test_dgd(rosetta):
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_ids = ['CTD:Lisinopril', 'PHAROS.DRUG:128029', 'PUBCHEM:5362119']
    disease_name = 'test_disease'
    disease_name_node = KNode('{}.{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    disease_ids = ['DOID:4325']
    query = UserQuery(drug_ids, node_types.DRUG, drug_name_node)
    query.add_transition(node_types.GENE)
    query.add_transition(node_types.DISEASE, end_values=disease_ids)
    query.add_end_lookup_node(disease_name_node)
    assert query.compile_query(rosetta)


def build_question2(drug_name, disease_name, drug_ids, disease_ids):
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    disease_name_node = KNode('{}.{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    query = UserQuery(drug_ids, node_types.DRUG, drug_name_node)
    query.add_transition(node_types.GENE)
    query.add_transition(node_types.PROCESS)
    query.add_transition(node_types.CELL)
    query.add_transition(node_types.ANATOMY)
    query.add_transition(node_types.PHENOTYPE)
    query.add_transition(node_types.DISEASE, end_values=disease_ids)
    return query


def test_query_two_from_builder(rosetta):
    drug_name = 'test_drug'
    drug_name_node = KNode('{}.{}'.format(node_types.DRUG_NAME, drug_name), node_types.DRUG_NAME)
    drug_ids = ['CTD:Adapalene', 'PHAROS.DRUG:95769', 'PUBCHEM:60164']
    disease_name = 'test_disease'
    disease_name_node = KNode('{}.{}'.format(node_types.DISEASE_NAME, disease_name), node_types.DISEASE_NAME)
    disease_ids = ['DOID:123']
    query = build_question2(drug_name, disease_name, drug_ids, disease_ids)
    assert query.compile_query(rosetta)
