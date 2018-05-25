DRUG='chemical_substance'
GENE='gene'
PATHWAY='pathway'
PROCESS='biological_process'
FUNCTION='molecular_function'
PROCESS_OR_FUNCTION='biological_process_or_molecular_activity'
CELL='cell'
ANATOMY='anatomical_entity'
PHENOTYPE='phenotypic_feature'
DISEASE='disease'
DISEASE_OR_PHENOTYPE='disease_or_phenotypic_feature'
GENETIC_CONDITION='genetic_condition'
UNSPECIFIED = 'UnspecifiedType'

#The root of all biolink_model entities, which every node in neo4j will also have as a label.
#used to specify constraints/indices
ROOT_ENTITY = 'named_thing'

node_types = set([DRUG, GENE, PATHWAY, PROCESS, FUNCTION, PROCESS_OR_FUNCTION, CELL, ANATOMY, PHENOTYPE, DISEASE, DISEASE_OR_PHENOTYPE, GENETIC_CONDITION, UNSPECIFIED])

type_codes = { 'S': DRUG, 'G':GENE, 'P':PROCESS_OR_FUNCTION, 'C':CELL, 'A':ANATOMY, 'T':PHENOTYPE, 'D':DISEASE, 'X':GENETIC_CONDITION , 'W': PATHWAY, '?': UNSPECIFIED}
