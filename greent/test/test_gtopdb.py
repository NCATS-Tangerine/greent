import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.graph_components import LabeledID
from greent.conftest import rosetta


@pytest.fixture()
def gtopdb(rosetta):
    g = rosetta.core.gtopdb
    return g

def test_vasopressin_precursor(gtopdb):
    input_node = KNode("GTOPDB:2168", type=node_types.CHEMICAL_SUBSTANCE, name="Vasopressin")
    results = gtopdb.chem_to_precursor(input_node)
    assert len(results) == 1
    for edge,node in results:
        assert node.type == node_types.GENE
        assert node.id == 'HGNC:894'

def test_vasopressin_genes(gtopdb):
    input_node = KNode("GTOPDB:2168", type=node_types.CHEMICAL_SUBSTANCE, name="Vasopressin")
    results = gtopdb.ligand_to_gene(input_node)
    assert len(results) == 4
    for edge,node in results:
        assert node.type == node_types.GENE

