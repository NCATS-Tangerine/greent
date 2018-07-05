from crawler.genes import load_genes
from crawler.chemicals import load_chemicals
from greent.rosetta import Rosetta

def crawl():
#    rosetta = Rosetta()
#    load_genes(rosetta)
    rosetta=''
    load_chemicals(rosetta)

if __name__=='__main__':
    crawl()
