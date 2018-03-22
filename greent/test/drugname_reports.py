from greent.services.chembio import ChemBioKS
from greent.services.ctd import CTD
from greent.services.pharos import Pharos
from greent.graph_components import KNode
from greent import node_types

#Todo: clean up the commonalities here, this is just a dump from other places.

def check_all_drugnames_chembio():
    from greent.service import ServiceContext
    cb = ChemBioKS(ServiceContext.create_context())
    with open('test/q2-drugandcondition-list.txt','r') as inf:
        h = inf.readline()
        uniq = set()
        for line in inf:
            x = line.split('\t')[0]
            uniq.add(x)
    n_no_pub = 0
    n_no_ncbi = 0
    for name in uniq:
        input_node = KNode("DRUG_NAME:{}".format(name), node_types.DRUG_NAME)
        try:
            drug_node = cb.graph_drugname_to_pubchem(input_node)[0][1]
            ident = drug_node.identifier
            ncbi_nodes = cb.graph_pubchem_to_ncbigene( drug_node )
            if len(ncbi_nodes) == 0:
                n_no_ncbi += 1
        except:
            n_no_pub += 1
            ident=''
            ncbi_nodes=[]
        print('{}\t{}\t{}'.format(name, ident, len(ncbi_nodes) ))
    print( '{} drugs'.format(len(uniq)) )
    print( '{} without pubchem id'.format(n_no_pub) )
    print( '{} without genes'.format(n_no_ncbi) )
    ngood = len(uniq) - n_no_pub - n_no_ncbi
    print( '{} good ({})'.format( ngood, ngood/len(uniq) ) )

def test_all_drugs_ctd():
    from greent.service import ServiceContext
    ctd = CTD(ServiceContext.create_context())
    with open('q2-drugandcondition-list.txt', 'r') as inf:
        h = inf.readline()
        uniq = set()
        for line in inf:
            x = line.split('\t')[0]
            uniq.add(x)
    n_no_ctd = 0
    n_no_gene = 0
    for name in uniq:
        input_node = KNode("DRUG_NAME:{}".format(name), node_types.DRUG_NAME)
        results = ctd.drugname_to_ctd(input_node)
        try:
            drug_node = results[0][1]
            ident = drug_node.identifier
        except:
            n_no_ctd += 1
            ident = ''
            gene_nodes = []
        if ident != '':
            gene_nodes = ctd.drug_to_gene(drug_node)
            if len(gene_nodes) == 0:
                n_no_gene += 1
        print('{}\t{}\t{}\t{}'.format(name, ident, len(results), len(gene_nodes)))
    print('{} drugs'.format(len(uniq)))
    print('{} without pubchem id'.format(n_no_ctd))
    print('{} without genes'.format(n_no_gene))
    ngood = len(uniq) - n_no_ctd - n_no_gene
    print('{} good ({})'.format(ngood, ngood / len(uniq)))

def test_all_drugs_pharos():
    from greent.service import ServiceContext
    pharos = Pharos(ServiceContext.create_context())
    with open('q2-drugandcondition-list.txt', 'r') as inf:
        h = inf.readline()
        uniq = set()
        for line in inf:
            x = line.split('\t')[0]
            uniq.add(x)
    n_no_pharos = 0
    n_no_hgnc = 0
    for name in uniq:
        input_node = KNode("DRUG_NAME:{}".format(name), node_types.DRUG_NAME)
        try:
            results = pharos.drugname_to_pharos(input_node)
            #print(name, results)
            drug_node = results[0][1]
            ident = drug_node.identifier
            hgnc_nodes = pharos.drug_get_gene(drug_node)
            if len(hgnc_nodes) == 0:
                n_no_hgnc += 1
        except:
            # print ('Not finding {}'.format(name))
            # exit()
            n_no_pharos += 1
            ident = ''
            hgnc_nodes = []
        print('{}\t{}\t{}\t{}'.format(name, ident, len(results), len(hgnc_nodes)))
    print('{} drugs'.format(len(uniq)))
    print('{} without pubchem id'.format(n_no_pharos))
    print('{} without genes'.format(n_no_hgnc))
    ngood = len(uniq) - n_no_pharos - n_no_hgnc
    print('{} good ({})'.format(ngood, ngood / len(uniq)))
