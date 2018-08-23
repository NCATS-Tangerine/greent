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

