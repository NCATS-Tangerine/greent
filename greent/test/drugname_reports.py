from greent.services.chembio import ChemBioKS
from greent.graph_components import KNode
from greent import node_types

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