import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.conftest import rosetta

def test_output_filter(rosetta):
    """Test automatic wrapping of gene_get_disease so that outputs are only genetic conditions"""
    fname='caster.output_filter(biolink~gene_get_disease,genetic_condition,mondo~is_genetic_disease)'
    func = rosetta.get_ops(fname)
    assert func is not None
    #PPARG
    results = func(KNode('HGNC:9236',node_types.GENE))
    for edge,node in results:
        assert node.node_type == node_types.GENETIC_CONDITION
    gc_identifiers = [ node.identifier for edge,node in results]
    #These are genetic conditions associated with PPARG
    assert 'MONDO:0007455' in gc_identifiers
    assert 'MONDO:0011448' in gc_identifiers
    assert 'MONDO:0018883' in gc_identifiers
    assert 'MONDO:0019245' in gc_identifiers
    #These are diseases associated with PPARG that are not Genetic Conditions (non-comprehensive)
    assert 'MONDO:0001106' not in gc_identifiers

def test_upcast(rosetta):
    """According to biolink model, pathway is_a biological process.  So we should be able to do gene->pathway and
    then upcast the result to type biological process"""
    fname='caster.upcast(biolink~gene_get_pathways,biological_process)'
    func = rosetta.get_ops(fname)
    assert func is not None
    #PPARG
    results = func(KNode('HGNC:9236',node_types.GENE))
    assert len(results) > 0
    for edge,node in results:
        assert node.node_type == node_types.PROCESS

def test_null_input_filter(rosetta):
    """According to biolink model, pathway is_a biological process.  So if we have a process, we can try to
    call pathway_get_gene, We don't have a function for determining whether an entity is a pathway, so the
    filter is a passthrough."""
    fname='caster.input_filter(biolink~pathway_get_gene,pathway)'
    func = rosetta.get_ops(fname)
    assert func is not None
    results = func(KNode('KEGG-path:maphsa04211',node_types.PROCESS)) #one of the results from the above
    assert len(results) > 0
    for edge,node in results:
        assert node.node_type == node_types.GENE
    gene_ids = [ node.identifier for edge,node in results]
    assert 'HGNC:9236' in gene_ids #PPARG

def test_input_filter(rosetta):
    """We have a typecheck for sell, so we can check for cell->something push_ups. IN this case the only
    cell->something function we have is cell->anatomy, and anatomy is also the superclass, so it gets a little
    confusing.  But basically, this is where we are going to have an anatomy node containing a cell identifier,
    and we should be able to call the function.   But when we call it with a non-cell anatomy, nothing will happen"""
    fname='caster.input_filter(uberongraph~get_anatomy_by_cell_graph,cell,typecheck~is_cell)'
    func = rosetta.get_ops(fname)
    assert func is not None
    results = func(KNode('CL:0000169',node_types.ANATOMY)) #Type-B pancreatic cell cast as an anatomy
    assert len(results) > 0
    anat_ids = [node.identifier for edge,node in results]
    assert 'UBERON:0001264' in anat_ids #pancreas
    results = func(KNode('UBERON:0001264',node_types.ANATOMY)) #PANCREAS
    assert len(results) == 0

def test_nested(rosetta):
    """cell->anatomy should be callable even if the input is upcast to an anatomy and we want a cell as output"""
    fname = 'caster.output_filter(input_filter(uberongraph~get_anatomy_by_cell_graph,cell,typecheck~is_cell),cell,typecheck~is_cell)'
    #Test that we can actually get a function.  This was failing 4/21
    func = rosetta.get_ops(fname)
