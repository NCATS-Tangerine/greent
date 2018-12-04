import pytest
from greent import node_types
from greent.graph_components import KNode,LabeledID
from greent.conftest import rosetta
from builder.obh_builder import ObesityHubBuilder

@pytest.fixture()
def obh(rosetta):
	return ObesityHubBuilder(rosetta, debug=True)

def test_cache(rosetta):
    #cached_phenotype = rosetta.cache.get('gwascatalog.sequence_variant_to_phenotype(CAID:CA6038253)')
    #cached_synon = rosetta.cache.get('synonymize(HGVS:NC_000001.10:g.13543del)')
    #assert cached_synon == set()
    pass

def test_metabolite_loader(rosetta, obh):
	metabolite_nodes, metabolite_file_names = obh.load_metabolite_info('./sample_metabolites.csv')
	assert len(metabolite_nodes) == 171

	assert obh.metabolite_label_to_node_lookup['1-methylguanidine'].id == 'PUBCHEM:10111'
	assert obh.metabolite_label_to_node_lookup['1-palmitoyl-GPI (16:0)'].id == 'HMDB:HMDB61695'

def test_sugen_file_reader(rosetta, obh):

	assert obh.quality_control_check('sample_sugen1', .00000001, .525, 100, delimiter=' ') == True
	assert obh.quality_control_check('sample_sugen1', .00000001, .4, 100, delimiter=' ') == False
	
	assert obh.quality_control_check('sample_sugen2', .00000001, .525, 100, delimiter=' ') == True
	assert obh.quality_control_check('sample_sugen2', 1e-5, .525, 1, delimiter=' ') == False
	assert obh.quality_control_check('sample_sugen2', 1e-20, .525, 1, delimiter=' ') == True


	labled_ids, p_values = obh.get_hgvs_identifiers_from_gwas('sample_sugen1', .000001, 'GRCh37', 'p1')
	assert len(labled_ids) == 0
	assert len(p_values.keys()) == 0

	labled_ids, p_values = obh.get_hgvs_identifiers_from_gwas('sample_sugen2', .000001, 'GRCh37', 'p1')
	hgvs_ids = [n.identifier for n in labled_ids]
	assert 'HGVS:NC_000023.10:g.32407761G>A' in hgvs_ids
	assert float(p_values['HGVS:NC_000023.10:g.32407761G>A']) < .00001
	assert 'HGVS:NC_000001.10:g.10235_10236insA' in hgvs_ids
	assert float(p_values['HGVS:NC_000001.10:g.10235_10236insA']) < .000001

	labled_ids, p_values = obh.get_hgvs_identifiers_from_gwas('sample_sugen3', .000001, 'GRCh37', 'p1')
	assert len(labled_ids) == 7

	labled_ids, p_values = obh.get_hgvs_identifiers_from_gwas('sample_sugen3', 1, 'GRCh37', 'p1')
	assert len(labled_ids) == 40

