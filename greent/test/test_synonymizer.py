import pytest
from greent import node_types
from greent.synonymizers import hgnc_synonymizer, disease_synonymizer, substance_synonymizer, cell_synonymizer, oxo_synonymizer
from greent.conftest import rosetta

def test_which_one(rosetta):
    synonymizer = rosetta.synonymizer
    synonymizer_map = synonymizer.synonymizers
    concept_model = rosetta.type_graph.concept_model
    expected = {
        node_types.GENE:set([hgnc_synonymizer]),
        node_types.DISEASE:set([disease_synonymizer]),
        node_types.CHEMICAL_SUBSTANCE:set([substance_synonymizer]),
        node_types.CELL:set([cell_synonymizer]),
        node_types.GENETIC_CONDITION:set([disease_synonymizer]),
        node_types.DRUG:set([substance_synonymizer]),
        node_types.METABOLITE:set([substance_synonymizer]),
        node_types.PHENOTYPIC_FEATURE:set([oxo_synonymizer]),
        node_types.DISEASE_OR_PHENOTYPIC_FEATURE:set([disease_synonymizer,oxo_synonymizer]),
        node_types.PATHWAY:set([oxo_synonymizer]),
        node_types.BIOLOGICAL_PROCESS:set([oxo_synonymizer]),
        node_types.MOLECULAR_ACTIVITY:set([oxo_synonymizer]),
        node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY:set([oxo_synonymizer]),
        node_types.ANATOMICAL_ENTITY:set([oxo_synonymizer, cell_synonymizer])
    }
    for ntype, slist in expected.items():
        assert slist == synonymizer_map[ntype]



