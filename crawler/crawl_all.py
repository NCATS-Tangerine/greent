from crawler.genes import load_genes
from greent.rosetta import Rosetta
from greent import node_types
from crawler.chemicals import load_chemicals
from crawler.program_runner import load_all
from crawler.disease_phenotype import load_diseases_and_phenotypes
from crawler.omni import create_omnicache,update_omnicache
from datetime import datetime as dt

def poolrun(type1,type2,rosetta):
    start = dt.now()
    psize = 10
    load_all(type1,type2,rosetta,psize)
    end = dt.now()
    print(f'Poolsize: {psize}, time: {end-start}')

def crawl():
    rosetta = Rosetta()
    #load_genes(rosetta)
    #load_chemicals(rosetta,refresh=False)
    #load_chemicals(rosetta,refresh=True)
    #load_diseases_and_phenotypes(rosetta)
    create_omnicache(rosetta)
    #poolrun(node_types.CHEMICAL_SUBSTANCE, node_types.DISEASE, rosetta)
    #poolrun(node_types.CHEMICAL_SUBSTANCE, node_types.PHENOTYPIC_FEATURE, rosetta)
    #poolrun(node_types.DISEASE, node_types.PHENOTYPIC_FEATURE, rosetta)
    #poolrun(node_types.DISEASE, node_types.GENE, rosetta)
    #poolrun(node_types.GENE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    #poolrun(node_types.CHEMICAL_SUBSTANCE, node_types.CHEMICAL_SUBSTANCE, rosetta)
    #poolrun(node_types.GENETIC_CONDITION, node_types.PHENOTYPIC_FEATURE, rosetta)
    #poolrun(node_types.PHENOTYPIC_FEATURE, node_types.DISEASE, rosetta)
    #poolrun(node_types.GENE, node_types.CHEMICAL_SUBSTANCE, rosetta)
    #poolrun(node_types.ANATOMICAL_ENTITY, node_types.PHENOTYPIC_FEATURE, rosetta)
    #poolrun(node_types.ANATOMICAL_ENTITY, node_types.CELL, rosetta)
    #poolrun(node_types.CELL, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    #poolrun(node_types.DISEASE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    #poolrun(node_types.DISEASE, node_types.CHEMICAL_SUBSTANCE, rosetta)

#
# MATCH (n)
# WHERE size((n)--())=0
# DELETE (n)
#

if __name__=='__main__':
    crawl()
