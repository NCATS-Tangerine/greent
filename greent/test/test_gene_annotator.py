import pytest
from greent.graph_components import KNode,LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta
from greent.annotators import annotator_factory
from greent.annotators.gene_annotator import GeneAnnotator

@pytest.fixture()
def gene_annotator(rosetta):
    gene_annotator = GeneAnnotator(rosetta)
    return gene_annotator

def test_ensembl_gene_annotation(gene_annotator):
    all_ensembl_annotations = gene_annotator.get_all_ensembl_gene_annotations()
    assert len(all_ensembl_annotations) > 65000

    single_gene_annotations = gene_annotator.get_ensembl_gene_annotations('ENSG00000100714')
    assert single_gene_annotations['ensembl_name'] == 'MTHFD1'
    assert single_gene_annotations['chromosome'] == '14'
    assert single_gene_annotations['start_position'] == 64388031
    assert single_gene_annotations['gene_biotype'] == 'protein_coding'

    single_gene_annotations = gene_annotator.get_ensembl_gene_annotations('ENSG00000177000')
    assert single_gene_annotations['ensembl_name'] == 'MTHFR'
    assert single_gene_annotations['chromosome'] == '1'
    assert single_gene_annotations['end_position'] == 11806920
    assert single_gene_annotations['gene_biotype'] == 'protein_coding'

def test_combined_gene_annotation(gene_annotator):
    # gene_annotator.annotate - these are coming from the cache after the first time

    gene_node = KNode('HGNC:9604', type=node_types.GENE)
    gene_node.add_synonyms( set( [LabeledID(identifier='ENSEMBL:ENSG00000095303', label='PTGS1')]))
    gene_annotator.annotate(gene_node)
    # these are from ensembl
    assert gene_node.properties['ensembl_name'] == 'PTGS1'
    assert gene_node.properties['chromosome'] == '9'
    # these are from hgnc
    assert gene_node.properties['location'] == '9q33.2'

    gene_node = KNode('HGNC:13089', type=node_types.GENE)
    gene_node.add_synonyms( set( [LabeledID(identifier='ENSEMBL:ENSG00000166526', label='ZNF3')]))
    gene_annotator.annotate(gene_node)
    # these are from ensembl
    assert gene_node.properties['ensembl_name'] == 'ZNF3'
    assert gene_node.properties['chromosome'] == '7'
    # these are from hgnc
    assert 'Zinc fingers C2H2-type' in gene_node.properties['gene_family']
    assert 28 in gene_node.properties['gene_family_id']

    gene_node = KNode('HGNC:122', type=node_types.GENE)
    gene_node.add_synonyms( set( [LabeledID(identifier='ENSEMBL:ENSG00000143727', label='ACP1')]))
    gene_annotator.annotate(gene_node)
    # these are from ensembl
    assert gene_node.properties['ensembl_name'] == 'ACP1'
    assert gene_node.properties['chromosome'] == '2'
    # these are from hgnc
    assert 1071 in gene_node.properties['gene_family_id']