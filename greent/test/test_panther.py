import pytest
from greent.graph_components import KNode, LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta



@pytest.fixture()
def panther(rosetta):
    panther = rosetta.core.panther
    return panther



def test_get_family_data(panther):
    data = panther.get_gene_family_data()
    assert data != None

def test_gene_family_data(panther):
    data = panther.gene_family_data

    assert 'PTHR11003' in data
    assert 'SF241' in data['PTHR11003']
    assert 'POTASSIUM CHANNEL, SUBFAMILY K' == data['PTHR11003']['family_name']
    assert 'POTASSIUM CHANNEL SUBFAMILY K MEMBER 5' == data['PTHR11003']['SF241']['sub_family_name']

def test_get_biological_process_by_gene_family(panther):
    top_family_node = KNode('PTHR11003', type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL, SUBFAMILY K')
    sub_family_node = KNode('PTHR11003:SF241',type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL, SUBFAMILY K Member 5' )
    
    response = panther. get_biological_process_or_activity_by_gene_family(top_family_node)
    node_ids = [ relation[1].id for relation in response ] 
    for edge, node in response:
        assert node.type == node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY
     #molecular activity
    assert 'GO:0005261' in node_ids
    #biological process
    assert 'GO:0006811' in node_ids
    response = panther.get_biological_process_or_activity_by_gene_family(sub_family_node)
    node_ids = [ relation[1].id for relation in response ] 
    for edge, node in response:
        assert node.type == node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY  
    #molecular activity
    assert 'GO:0005261' in node_ids
    #biological process
    assert 'GO:0006811' in node_ids
   
def test_get_cellular_component_by_gene_family(panther):    
    top_family_node = KNode('PTHR11003', type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL, SUBFAMILY K')
    sub_family_node = KNode('PTHR11003:SF241',type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL, SUBFAMILY K Member 5' )
    response = panther. get_cellular_component_by_gene_family(top_family_node)
    node_ids = [ relation[1].id for relation in response ]
    for edge, node in response:
        assert node.type == node_types.CELLULAR_COMPONENT  
    assert 'GO:0044464' in node_ids
    response = panther. get_cellular_component_by_gene_family(sub_family_node)
    node_ids = [ relation[1].id for relation in response ]    
    assert 'GO:0044464' in node_ids

def test_get_pathway_by_gene_family(panther):
    top_family_node = KNode('PTHR11003', type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL, SUBFAMILY K')
    sub_family_node = KNode('PTHR11003:SF75',type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL SUBFAMILY K MEMBER 9' )
    response = panther. get_pathway_by_gene_family(top_family_node)
    node_ids = [ relation[1].id for relation in response ]
    assert 'P04376' in node_ids
    

def test_get_gene_by_gene_family(panther):
    top_family_node = KNode('PTHR11003', type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL, SUBFAMILY K')
    sub_family_node = KNode('PTHR11003:SF75',type= node_types.GENE_FAMILY, name='POTASSIUM CHANNEL SUBFAMILY K MEMBER 9' )
    response = panther. get_gene_by_gene_family(top_family_node)
    node_ids = [ relation[1].id for relation in response ] 
    assert 'HGNC:6283' in node_ids
    response = panther. get_gene_by_gene_family(top_family_node)
    node_ids = [ relation[1].id for relation in response ] 
    assert 'HGNC:6283' in node_ids
    
def test_get_gene_family_by_gene_family(panther):
    pass