import pytest
from greent.graph_components import KNode
from greent.services.hmdb_beacon import HMDB
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta


@pytest.fixture()
def hmdb(rosetta):
    hmdb = rosetta.core.hmdb
    return hmdb

def test_disease_to_metabolite(hmdb):
    asthma = KNode('UMLS:C0004096', type=node_types.DISEASE)
    results = hmdb.disease_to_metabolite(asthma)
    assert len(results) > 0
    node_labels=[node.name for edge,node in results]
    assert '5-HETE' in node_labels

def test_enzyme_to_metabolite(hmdb):
    asthma = KNode('UniProtKB:Q96SL4', type=node_types.GENE)
    results = hmdb.enzyme_to_metabolite(asthma)
    assert len(results) > 0
    node_labels=[node.name for edge,node in results]
    assert '5-HETE' in node_labels

def test_pathway_to_metabolite(hmdb):
    pathway = KNode('SMPDB:SMP00710', type=node_types.PATHWAY)
    results = hmdb.pathway_to_metabolite(pathway)
    assert len(results) > 0
    #make sure we got a metabolite we expect
    node_labels=[node.name for edge,node in results]
    assert '5-HETE' in node_labels
    #we're looking up by pathway, but pathway should be the object, is it?
    for edge,node in results:
        assert edge.target_id == pathway.id


def test_metabolite_to_disease(hmdb):
    hete = KNode('HMDB:HMDB0011134', type=node_types.DRUG)
    results = hmdb.metabolite_to_disease(hete)
    assert len(results) > 0
    node_labels=[node.name for edge,node in results]
    assert 'Asthma' in node_labels

def test_metabolite_to_enzyme(hmdb):
    hete = KNode('HMDB:HMDB0011134', type=node_types.DRUG)
    results = hmdb.metabolite_to_enzyme(hete)
    assert len(results) > 0
    node_ids=[node.id for edge,node in results]
    assert 'UniProtKB:Q96SL4' in node_ids

def test_metabolite_to_pathway(hmdb):
    hete = KNode('HMDB:HMDB0011134', type=node_types.DRUG)
    results = hmdb.metabolite_to_pathway(hete)
    assert len(results) > 0
    node_ids=[node.id for edge,node in results]
    assert 'SMPDB:SMP00710' in node_ids

