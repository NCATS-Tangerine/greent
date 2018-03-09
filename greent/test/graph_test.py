import pytest
from greent.graph import TypeGraph
from greent.service import ServiceContext

#@pytest.fixture(scope='module')
#def rosetta():
#    from greent.rosetta import Rosetta
#    return Rosetta()

def test_one_sided():
    cypher='''MATCH p=
    (c0:Concept {name: "Disease" })
    --
    (c1:Concept {name: "Gene" })
    --
    (c2:Concept {name: "GeneticCondition" })
    WITH p,c0,c2
    MATCH q=(c0:Concept)-[:translation*0..2]->(c2:Concept)
    WHERE p=q
    RETURN p, EXTRACT( r in relationships(p) | startNode(r)) '''
    graph = TypeGraph(ServiceContext.create_context())
    plans = graph.get_transitions(cypher)
    #There's one execution plan
    assert len(plans) == 1
    #it consists of 3 nodes and 2 transitions
    nodes, transitions = plans[0]
    assert len(nodes) == 3
    assert len(transitions) == 2
    #The nodes go Disease, Gene, GeneticCondition
    assert nodes[0] == "Disease"
    assert nodes[1] == "Gene"
    assert nodes[2] == "GeneticCondition"
    #The relations point 0->1->2
    assert transitions[0]['to']==1
    assert transitions[1]['to']==2

def test_two_sided():
    cypher='''MATCH 
    q=(x:Concept{name: "Substance"})--
    (w0:Concept{name:   "Gene"})-
    [r0*1..2]-
    (w1:Concept{name:   "Cell"})-
    [r1*1..2]-
    (w2:Concept{name:   "Phenotype"})--
    (y:Concept{name:   "Disease"})
    with q,x,y MATCH
    p=(x:Concept)-[zebra*0..6]->()<-[macaroni*0..6]-(y:Concept)
    where p=q
    return p, EXTRACT( r in relationships(p) | startNode(r)) '''
    graph = TypeGraph(ServiceContext.create_context())
    plans = graph.get_transitions(cypher)
    #There's three execution plans, because there are three different ways to go from substance to gene
    #But they're the same in connectivity and types.
    assert len(plans) == 3
    #it consists of 3 nodes and 2 transitions
    for nodes,transitions in plans:
        assert len(nodes) == 7
        assert len(transitions) == 6
        #The nodes go Disease, Gene, GeneticCondition
        assert nodes[0] == "Substance"
        assert nodes[1] == "Gene"
        assert nodes[2] == "BiologicalProcess"
        assert nodes[3] == "Cell"
        assert nodes[4] == "Anatomy"
        assert nodes[5] == "Phenotype"
        assert nodes[6] == "Disease"
        #The relations point 0->1->2
        assert transitions[0]['to']==1
        assert transitions[1]['to']==2
        assert transitions[2]['to']==3
        assert transitions[3]['to']==4
        assert transitions[5]['to']==4
        assert transitions[6]['to']==5
