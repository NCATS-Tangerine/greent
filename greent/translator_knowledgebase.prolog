is_a(tS,  drug).
is_a(tG,  gene).
is_a(tP,  pathway).
is_a(tA,  cell).
is_a(tPH, phenotype).
is_a(tD,  disease).
is_a(tGC, genetic_condition).

has_context(disease, [ mesh_disease_name, mesh_disease_id, doid ]).
has_context(drug,    [ mesh_drug_name, cbr_drug_name, cbr_drugbank_id ]).
has_context(gene,    [ cbr_gene, hgnc_id ]).
has_context(pathway, [ cbr_pathway ]).
has_context(cell,    [ hetio_cell ]).
has_context(anatomy, [ hetio_anatomy ]).

is_type(thing,               "http://identifiers.org/doi").
is_type(mesh,                "http://identifiers.org/mesh").
is_type(mesh_disease_name,   "http://identifiers.org/mesh/disease/name").
is_type(mesh_drug_name,      "http://identifiers.org/mesh/drug/name").
is_type(mesh_disease_id,     "http://identifiers.org/mesh/disease/id").
is_type(doid,                "http://identifiers.org/doid").
is_type(pharos_disease_name, "http://pharos.nih.gov/identifier/disease/name").
is_type(pharos_disease_id,   "http://pharos.nih.gov/identifier/disease/id").
is_type(cbr_drug_name,       "http://chem2bio2rdf.org/drugbank/resource/Generic_Name").
is_type(cbr_gene,            "http://chem2bio2rdf.org/uniprot/resource/gene").
is_type(cbr_pathway,         "http://chem2bio2rdf.org/kegg/resource/kegg_pathway").
is_type(hetio_cell,          "http://identifier.org/hetio/cellcomponent").
is_type(hetio_anatomy,       "http://identifier.org/hetio/anatomy").
is_type(hgnc_id,             "http://identifiers.org/hgnc").

translates(cbr_gene,          cbr_pathway,       "chembio_ks.gene_symbol_to_pathway").
translates(cbr_gene,          cbr_pathway,       "pharos.target_to_disease").
translates(cbr_gene,          hetio_anatomy,     "hetio.gene_to_anatomy").
translates(cbr_gene,          hetio_cell,        "hetio.gene_to_cell").
translates(doid,              mesh_disease_id,   "disease_ontology.doid_to_mesh").
translates(doid,              cbr_gene,          "pharos.disease_to_target").
translates(doid,              pharos_disease_id, "disease_ontology.doid_to_pharos").
translates(mesh_disease_name, mesh_drug_name,    "chemotext.disease_name_to_drug_name").
translates(mesh,              thing,             "chemotext.term_to_term").
translates(pharos_disease_id, hgnc_id,           "pharos.get_target_hgnc").
translates(cbr_drug_name,     cbr_gene,          "chembio_ks.drug_name_to_gene_symbol").
translates(pharos_disease_id, hgnc_gene,         "pharos.disease_to_target").

reify(Concept,Specific,Types) :-
    is_a(Concept,Specific),
    has_context(Specific,Types).

type_matrix(ConceptA,SpecificA,TypesA,ConceptB,SpecificB,TypesB) :-
    reify(ConceptA,SpecificA,TypesA),
    reify(ConceptB,SpecificB,TypesB).

abstract_to_transitions(ConceptA,SpecificA,TypesA,ConceptB,SpecificB,TypesB, []) :-
    type_matrix(ConceptA,SpecificA,TypesA,ConceptB,SpecificB,TypesB,[]).
abstract_to_transitions(ConceptA,SpecificA,TypesA,ConceptB,SpecificB,TypesB, [H|T]) :-
    type_matrix(ConceptA,SpecificA,[H1|T1],ConceptB,SpecificB,[H2|T2],[XH|XT]),
    path_to(H1,H2,XH),
    abstract_to_transitions(ConceptA,SpecificA,T1,ConceptB,SpecificB,T2, XT).
    
path_to(X,Y,[M]) :-
    translates(X,Y,M).
path_to(X,Y,[H|T]) :-
    translates(X,Z,H),
    path_to(Z,Y,T).
