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
    #logsyn
    #load_genes(rosetta)
    #load_chemicals(rosetta,refresh=False)
    #load_diseases_and_phenotypes(rosetta)
    #logomni
    #create_omnicache(rosetta)
    #poolrun(node_types.DISEASE, node_types.GENE, rosetta)
    #log9
    #poolrun(node_types.PHENOTYPIC_FEATURE, node_types.DISEASE, rosetta)
    #poolrun(node_types.CHEMICAL_SUBSTANCE, node_types.DISEASE, rosetta)
    #poolrun(node_types.CHEMICAL_SUBSTANCE, node_types.PHENOTYPIC_FEATURE, rosetta)
    #poolrun(node_types.CHEMICAL_SUBSTANCE, node_types.CHEMICAL_SUBSTANCE, rosetta)
    #THIS ONE HAS SOME PMID ERRORS: check /scratch/bizon/log	
    #poolrun(node_types.DISEASE, node_types.PHENOTYPIC_FEATURE, rosetta)
    #log2
    #poolrun(node_types.GENETIC_CONDITION, node_types.PHENOTYPIC_FEATURE, rosetta)
    #log3
    #poolrun(node_types.GENE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    #log4
    #poolrun(node_types.ANATOMICAL_ENTITY, node_types.PHENOTYPIC_FEATURE, rosetta)
    #log5
    #poolrun(node_types.ANATOMICAL_ENTITY, node_types.CELL, rosetta)
    #log6
    #poolrun(node_types.CELL, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    #log7
    #poolrun(node_types.DISEASE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    #log8
    #poolrun(node_types.DISEASE, node_types.CHEMICAL_SUBSTANCE, rosetta)
    #poolrun(node_types.GENE, node_types.CHEMICAL_SUBSTANCE, rosetta)

if __name__=='__main__':
    crawl()
