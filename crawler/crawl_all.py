from crawler.genes import load_genes
from greent.rosetta import Rosetta

def crawl():
    rosetta = Rosetta()
    load_genes(rosetta)

if __name__=='__main__':
    crawl()
