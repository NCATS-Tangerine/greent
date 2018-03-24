import json
import pytest
from greent.graph_components import KNode
from greent.services.ctd import CTD
from greent.service import ServiceContext
from greent import node_types
from greent.util import Text
from greent.graph import TypeGraph

@pytest.fixture(scope='module')
def type_graph():
    return TypeGraph (ServiceContext.create_context())

@pytest.fixture(scope='module')
def query():
    return """
    MATCH p=
    (c0:Concept {name: "chemical_substance" })
    --
    (c1:Concept {name: "gene" })
    --
    (c2:Concept {name: "biological_process" })
    --
    (c3:Concept {name: "cell" })
    --
    (c4:Concept {name: "anatomical_entity" })
    --
    (c5:Concept {name: "phenotypic_feature" })
    --
    (c6:Concept {name: "disease" })
    FOREACH (n in relationships(p) | SET n.marked = TRUE)
    WITH p,c0,c6
    MATCH q=(c0:Concept)-[*0..6 {marked:True}]->()<-[*0..6 {marked:True}]-(c6:Concept)
    WHERE p=q
    AND ALL( r in relationships(p) WHERE  EXISTS(r.op) )FOREACH (n in relationships(p) | SET n.marked = FALSE)
    RETURN p, EXTRACT( r in relationships(p) | startNode(r) ) 
    """
def test_two_sided_query(type_graph, query, expected):
    transitions = type_graph.get_transitions (query)
    print (f"len of expected: {len(expected)}")
    assert len(expected) == 18
    for i, actual in enumerate(transitions):
        matched = False
        candidate = None
        for expect in expected:
            candidate = expect[1]
            for Lk, Lv in actual[1].items ():
                actual_key = str(Lk)
                if actual_key in candidate:
                    Rv = candidate[actual_key]
                    matched = Lv == Rv
                    if matched:
                        break
        if not matched:
            print (f"Failed to match actual: {json.dumps(actual,indent=2)}") #{json.dumps(candidate,indent=2)}")
        print (f"Matched all transitions in result {i}")
        assert matched

@pytest.fixture(scope='module')
def expected():
    return [
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "ctd.drug_to_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_genetic_condition",
                    "op": "mondo.doid_get_genetic_condition",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "pharos.drug_get_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_genetic_condition",
                    "op": "mondo.doid_get_genetic_condition",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "chemical_targets_gene",
                    "op": "chembio.graph_pubchem_to_ncbigene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_genetic_condition",
                    "op": "mondo.doid_get_genetic_condition",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "ctd.drug_to_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "hetio.disease_to_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "pharos.drug_get_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "hetio.disease_to_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "chemical_targets_gene",
                    "op": "chembio.graph_pubchem_to_ncbigene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "hetio.disease_to_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "ctd.drug_to_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "biolink.disease_get_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "pharos.drug_get_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "biolink.disease_get_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "chemical_targets_gene",
                    "op": "chembio.graph_pubchem_to_ncbigene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_annotation_extensions",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "biolink.disease_get_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "ctd.drug_to_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_genetic_condition",
                    "op": "mondo.doid_get_genetic_condition",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "pharos.drug_get_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_genetic_condition",
                    "op": "mondo.doid_get_genetic_condition",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "chemical_targets_gene",
                    "op": "chembio.graph_pubchem_to_ncbigene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_genetic_condition",
                    "op": "mondo.doid_get_genetic_condition",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "ctd.drug_to_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "hetio.disease_to_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "pharos.drug_get_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "hetio.disease_to_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "chemical_targets_gene",
                    "op": "chembio.graph_pubchem_to_ncbigene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "hetio.disease_to_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "ctd.drug_to_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "biolink.disease_get_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "drug_to_gene",
                    "op": "pharos.drug_get_gene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "biolink.disease_get_phenotype",
                    "to": 5
                }
            }
        ],
        [
            {
                "0": "chemical_substance",
                "1": "gene",
                "2": "biological_process",
                "3": "cell",
                "4": "anatomical_entity",
                "5": "phenotypic_feature",
                "6": "disease"
            },
            {
                "0": {
                    "link": "chemical_targets_gene",
                    "op": "chembio.graph_pubchem_to_ncbigene",
                    "to": 1
                },
                "1": {
                    "link": "biological_process",
                    "op": "biolink.gene_get_process",
                    "to": 2
                },
                "2": {
                    "link": "bio_process_cell_type",
                    "op": "quickgo.go_term_xontology_relationships",
                    "to": 3
                },
                "3": {
                    "link": "cell_type_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_cell_graph",
                    "to": 4
                },
                "5": {
                    "link": "phenotype_to_anatomy",
                    "op": "uberongraph.get_anatomy_by_phenotype_graph",
                    "to": 4
                },
                "6": {
                    "link": "disease_to_phenotype",
                    "op": "biolink.disease_get_phenotype",
                    "to": 5
                }
            }
        ]
    ]
