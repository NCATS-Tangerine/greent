import pytest
from greent import node_types
from greent.graph_components import KNode,LabeledID
from greent.conftest import rosetta
from builder.obh_builder import ObesityHubBuilder
from builder.obh_builder import get_ordered_names_from_csv

@pytest.fixture()
def obh(rosetta):
	return ObesityHubBuilder(rosetta, debug=True)

def a_test_cache(rosetta, obh):

	#num_cached = obh.prepopulate_variant_cache('./sample_sugen3')
	#assert num_cached == 40

	#obh.prepopulate_gwascatalog_cache()

    #cached_phenotype = rosetta.cache.get('gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(CAID:CA6038253)')
    #cached_synon = rosetta.cache.get('')
    #assert cached_synon != set()
	#cached_synon = rosetta.cache.get('myvariant.sequence_variant_to_gene(CAID:CA248392703)')
    #identifiers = [node.id for r,node in cached_synon]
    #assert 'HGNC:3316' in identifiers

	#cached_synon = rosetta.cache.get('gwascatalog.sequence_variant_to_disease_or_phenotypic_feature(CAID:CA248392703)')
	#assert cached_synon != set()

    pass

def test_metabolite_loader(rosetta, obh):
	metabolite_nodes, metabolite_file_names = obh.load_metabolite_info('./sample_metabolites.csv', file_names_postfix='_scale')
	assert len(metabolite_nodes) == 171

	assert obh.metabolite_labled_id_lookup['methylguanidine_std_scale'].identifier == 'PUBCHEM:10111'
	assert obh.metabolite_labled_id_lookup['palmitoylGPI160_std_scale'].identifier == 'HMDB:HMDB61695'
	assert not 'enylpalmitoyl2oleoylGPCP160181_std_scale' in obh.metabolite_labled_id_lookup

def test_mwas_file_reader(rosetta, obh):
	ordered_names = get_ordered_names_from_csv('./sample_mwas', 'TRAIT')
	assert ordered_names[0] == 'enylpalmitoyl2oleoylGPCP160181_std_scale'
	assert ordered_names[8] == 'enylpalmitoyl2linoleoylGPEP160182_std_scale'

	metabolite_nodes, metabolite_file_names = obh.load_metabolite_info('./sample_metabolites.csv', file_names_postfix='_scale')
	assert len(metabolite_nodes) == 171
	labled_ids, p_values = obh.get_metabolite_identifiers_from_mwas('./sample_mwas', 1e-5)
	assert len(labled_ids) == 3

def test_sugen_file_reader(rosetta, obh):

	assert obh.quality_control_check('./sample_sugen', p_value_threshold=.05, max_hits=1, delimiter='\t') == False
	assert obh.quality_control_check('./sample_sugen', p_value_threshold=.05, max_hits=100, delimiter='\t') == True
	assert obh.quality_control_check('./sample_sugen', delimiter='\t') == True

	# p value is too strict
	variant_info = obh.get_variants_from_gwas('./sample_sugen', .005, 'GRCh37', 'p1')
	assert len(variant_info) == 0

	# impute2 cutoff is too strict
	variant_info = obh.get_variants_from_gwas('./sample_sugen', .05, 'GRCh37', 'p1', impute2_cutoff=0.7)
	assert len(variant_info) == 0

	variant_info = obh.get_variants_from_gwas('./sample_sugen', .05, 'GRCh37', 'p1')
	assert len(variant_info) == 5

	variant_ids, p_values = zip(*variant_info)
	assert 'NC_000001.10:g.19299674_19299676del' in variant_ids
	assert .049 in p_values

def test_gwas_builder(rosetta, obh):
	#this will actually write to neo4j
	#create a graph with just one node / file
    #p_value_cutoff = 1e-5

    #pa_id = 'EFO:0001073'
    #pa_node = KNode(pa_id, name='Obesity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #associated_nodes = [pa_node]
    #associated_file_names = {pa_id: 'sample_gwas'}
    #gwas_directory = '.'
    #obh.create_gwas_graph(associated_nodes, associated_file_names, gwas_directory, p_value_cutoff, data_set_tag='testing_analysis')
   
    #pa_id = 'EFO:0001073'
    #pa_node = KNode(pa_id, name='Obesity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #associated_nodes = [pa_node]
    #associated_file_names = {pa_id: 'obese95_control5to50'}
    #gwas_directory = '/projects/sequence_analysis/vol1/obesity_hub/BMI/GWAS/aggregate_results'
    #obh.create_gwas_graph(associated_nodes, associated_file_names, gwas_directory, p_value_cutoff, data_set_tag='obesity_gwas')
    #pass
    #pa_node = KNode(pa_id, name='Obesity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #associated_nodes = [pa_node]
    #associated_file_names = {pa_id: 'obese95_control5to50_diet'}
    #obh.create_gwas_graph(associated_nodes, associated_file_names, gwas_directory, p_value_cutoff, data_set_tag='obesity_gwas_diet')
    pass


