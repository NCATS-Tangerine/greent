from crawler.genes import load_genes
from greent.rosetta import Rosetta
from crawler.chemicals import load_chemicals
from crawler.omni import create_omnicache

def crawl():
    rosetta = Rosetta()
    load_genes(rosetta)
    load_chemicals(rosetta)
#    create_omnicache(rosetta)

if __name__=='__main__':
    crawl()
