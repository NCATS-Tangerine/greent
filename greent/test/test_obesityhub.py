import pytest
from greent import node_types
from greent.graph_components import KNode,LabeledID
from greent.conftest import rosetta
from builder.obh_builder import ObesityHubBuilder

@pytest.fixture()
def obh(rosetta):
	return ObesityHubBuilder(rosetta, debug=True)

def test_sugen_file_reader(rosetta, obh):
	labled_ids, p_values = obh.get_hgvs_identifiers_from_vcf('sample_sugen1', .000001, 'GRCh37', 'p1')
	assert len(labled_ids) == 0
	assert len(p_values.keys()) == 0

	labled_ids, p_values = obh.get_hgvs_identifiers_from_vcf('sample_sugen2', .000001, 'GRCh37', 'p1')
	hgvs_ids = [n.identifier for n in labled_ids]
	assert 'HGVS:NC_000023.10:g.32407761G>A' in hgvs_ids
	assert float(p_values['NC_000023.10:g.32407761G>A']) < .00001
	assert 'HGVS:NC_000001.10:g.10235_10236insA' in hgvs_ids
	assert float(p_values['HGVS:NC_000001.10:g.10235_10236insA']) < .000001


	labled_ids, p_values = obh.get_hgvs_identifiers_from_vcf('sample_sugen3', .000001, 'GRCh37', 'p1')
	assert len(labled_ids) == 7

	labled_ids, p_values = obh.get_hgvs_identifiers_from_vcf('sample_sugen3', 1, 'GRCh37', 'p1')
	assert len(labled_ids) == 40

