import pytest
from greent import node_types
from greent.graph_components import KNode

# from greent.graph_components import LabeledID


@pytest.fixture()
def gtex(rosetta):
    gtex_ref = rosetta.code.gtex
    return gtex_ref


def test_gtex_cache_loaded(rosetta, gtex):
    cached = rosetta.cache.get('myvariant.sequence_variant_to_gene(CAID:CA248392703)')
    assert cached


def test_sequence_variant_to_gene(rosetta, gtex):
    # create a couple nodes
    variant_node = KNode("HGVS:NC_000001.10:g.178632324A>G", name="HGVS:NC_000001.10:g.178632324A>G")
    gene_node = KNode("ENSEMBL:ENSG00000116191")

    # call the func to make that association
    results = gtex.sequence_variant_to_gene(variant_node, gene_node)

    assert len(results) > 0

    # check to make sure the node and edges were created properly
    for edge, node in results:
        assert node.type == node_types.GENE or node.type == node_types.SEQUENCE_VARIANT
        assert edge.standard_predicate.identifier != 'GAMMA:0'


def test_sequence_variant_to_anatomy(rosetta, gtex):
    variant_node = KNode("HGVS:NC_000001.10:g.178632324A>G", name="HGVS:NC_000001.10:g.178632324A>G")
    gtex_node = KNode("UBERON:0002190", name="Adipose Subcutaneous")

    # call the func to make that association
    results = gtex.sequence_variant_to_anatomy(variant_node, gtex_node)

    assert len(results) > 0

    # check to make sure the node and edges were created properly
    for edge, node in results:
        assert node.type == node_types.SEQUENCE_VARIANT or node.type == node_types.ANATOMICAL_ENTITY
        assert edge.standard_predicate.identifier != 'GAMMA:0'


def test_gene_to_anatomy(rosetta, gtex):
    gene_node = KNode("ENSEMBL:ENSG00000116191")
    gtex_node = KNode("UBERON:0002190", name="Adipose Subcutaneous")

    # call the func to make that association
    results = gtex.gene_to_anatomy(gene_node, gtex_node)

    assert len(results) > 0

    # check to make sure the node and edges were created properly
    for edge, node in results:
        assert node.type == node_types.GENE or node.type == node_types.ANATOMICAL_ENTITY
        assert edge.standard_predicate.identifier != 'GAMMA:0'
    assert True


"""
def test_gtex_builder(rosetta, gtex):
    # this will actually write to neo4j
    # create a graph with just one node / file

    # load the redis cache
    gtb.prepopulate_gtex_catalog_cache()

    # directory with GTEx data to process
    gtex_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'

    # create a node
    gtex_id = '0002190'  # ex. uberon "Adipose Subcutaneous"
    gtex_node = KNode(gtex_id, name='gtex_tissue', type=node_types.ANATOMICAL_ENTITY)

    # assign the node to an array
    associated_nodes = [gtex_node]

    # assign the name of the GTEx data file
    associated_file_names = {'little_signif.csv'}

    # call the GTEx builder
    gtb.create_gtex_graph(associated_nodes, associated_file_names, gtex_directory, 'testing_gtex')
    pass
"""
