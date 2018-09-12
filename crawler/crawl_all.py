from crawler.genes import load_genes
from greent.rosetta import Rosetta
from greent import node_types
from crawler.chemicals import load_chemicals
from crawler.program_runner import load_all
from crawler.disease_phenotype import load_diseases_and_phenotypes
from crawler.omni import create_omnicache
from datetime import datetime as dt

def poolrun(type1,type2,rosetta):
    start = dt.now()
    psize =10 
    load_all(type1,type2,rosetta,psize)
    end = dt.now()
    print(f'Poolsize: {psize}, time: {end-start}')

def crawl():
    rosetta = Rosetta()
    #load_genes(rosetta)
    #load_chemicals(rosetta,refresh=False)
    #load_diseases_and_phenotypes(rosetta)
    #poolrun(node_types.DISEASE, node_types.PHENOTYPIC_FEATURE, rosetta)
    #poolrun(node_types.GENETIC_CONDITION, node_types.PHENOTYPIC_FEATURE, rosetta)
    poolrun(node_types.GENE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, rosetta)
    poolrun(node_types.DISEASE, node_types.GENE, rosetta)
    #create_omnicache(rosetta)

if __name__=='__main__':
    crawl()
