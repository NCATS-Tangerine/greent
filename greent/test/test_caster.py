import pytest
from greent.graph_components import KNode, LabeledID
from greent import node_types
from greent.conftest import rosetta


def test_kegg_get_enzyme(rosetta):
    fname='caster.input_filter(kegg~chemical_get_enzyme,metabolite)'
    func = rosetta.get_ops(fname)
    node = KNode('CHEBI:29073', name='chem',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(node)
    results = func(node)
    assert len(results) > 0

def test_kegg_get_enzyme_norcodeine(rosetta):
    fname='caster.input_filter(kegg~chemical_get_enzyme,metabolite)'
    func = rosetta.get_ops(fname)
    node = KNode('CHEBI:80579', name='norcodeine',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(node)
    results = func(node)
    for edge,node in results:
        print(edge,node.id,node.name)


def test_kegg(rosetta):
    fname='caster.upcast(kegg~enzyme_get_chemicals,chemical_substance)'
    func = rosetta.get_ops(fname)
    assert func is not None
    node = KNode('HGNC:2843', name='gene',type=node_types.GENE)
    rosetta.synonymizer.synonymize(node)
    results = func(node)
    assert len(results) > 0


def test_drugcentral(rosetta):
    fname='caster.output_filter(mychem~get_drugcentral,disease,typecheck~is_disease)'
    func = rosetta.get_ops(fname)
    assert func is not None
    node = KNode('CHEMBL:CHEMBL159', type=node_types.CHEMICAL_SUBSTANCE)
    results = func(node)
    for e,n in results:
        assert e.provided_by=='mychem.get_drugcentral'

def test_complicated(rosetta):
    """make sure that a very complicated cast gets everything to the right place"""
    fname='caster.output_filter(input_filter(upcast(hetio~disease_to_phenotype,disease_or_phenotypic_feature),disease,typecheck~is_disease),disease,typecheck~is_disease)'
    func = rosetta.get_ops(fname)
    assert func is not None
    node = KNode('HP:0007354', type=node_types.PHENOTYPIC_FEATURE)
    node.add_synonyms( set( [LabeledID(identifier='DOID:332', label='ALS')] ) )
    results = func(node)
    assert results is not None

def test_output_filter(rosetta):
    """Test automatic wrapping of gene_get_disease so that outputs are only genetic conditions"""
    fname='caster.output_filter(biolink~gene_get_disease,genetic_condition,mondo~is_genetic_disease)'
    func = rosetta.get_ops(fname)
    assert func is not None
    #PPARG
    results = func(KNode('HGNC:9236', type=node_types.GENE))
    for edge,node in results:
        assert node.type == node_types.GENETIC_CONDITION
    gc_identifiers = [ node.id for edge,node in results]
    #These are genetic conditions associated with PPARG
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
    results = func(KNode('HGNC:9236', type=node_types.GENE))
    assert len(results) > 0
    for edge,node in results:
        assert node.type == node_types.BIOLOGICAL_PROCESS

def test_null_input_filter(rosetta):
    """According to biolink model, pathway is_a biological process.  So if we have a process, we can try to
    call pathway_get_gene, We don't have a function for determining whether an entity is a pathway, so the
    filter is a passthrough."""
    fname='caster.input_filter(biolink~pathway_get_gene,pathway)'
    func = rosetta.get_ops(fname)
    assert func is not None
    results = func(KNode('KEGG-path:maphsa04211', type=node_types.BIOLOGICAL_PROCESS)) #one of the results from the above
    assert len(results) > 0
    for edge,node in results:
        assert node.type == node_types.GENE
    gene_ids = [ node.id for edge,node in results]
    assert 'HGNC:9236' in gene_ids #PPARG

def test_input_filter(rosetta):
    """We have a typecheck for sell, so we can check for cell->something push_ups. IN this case the only
    cell->something function we have is cell->anatomy, and anatomy is also the superclass, so it gets a little
    confusing.  But basically, this is where we are going to have an anatomy node containing a cell identifier,
    and we should be able to call the function.   But when we call it with a non-cell anatomy, nothing will happen"""
    fname='caster.input_filter(uberongraph~get_anatomy_by_cell_graph,cell,typecheck~is_cell)'
    func = rosetta.get_ops(fname)
    assert func is not None
    results = func(KNode('CL:0000169', type=node_types.ANATOMICAL_ENTITY)) #Type-B pancreatic cell cast as an anatomy
    assert len(results) > 0
    anat_ids = [node.id for edge,node in results]
    assert 'UBERON:0001264' in anat_ids #pancreas
    results = func(KNode('UBERON:0001264', type=node_types.ANATOMICAL_ENTITY)) #PANCREAS
    assert len(results) == 0

def test_nested(rosetta):
    """cell->anatomy should be callable even if the input is upcast to an anatomy and we want a cell as output"""
    fname = 'caster.output_filter(input_filter(uberongraph~get_anatomy_by_cell_graph,cell,typecheck~is_cell),cell,typecheck~is_cell)'
    #Test that we can actually get a function.  This was failing 4/21
    func = rosetta.get_ops(fname)

