from crawler.genes import load_genes
from crawler.omni import create_omnicache
from greent.rosetta import Rosetta

def crawl():
    rosetta = Rosetta()
    #load_genes(rosetta)
    create_omnicache(rosetta)

if __name__=='__main__':
    crawl()
