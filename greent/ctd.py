import json
import logging
import os
import requests
import time
from collections import defaultdict
from greent.service import Service
from greent.service import ServiceContext
from greent.util import LoggingUtil
from greent.graph_components import KNode, KEdge
from greent.util import Text
from greent import node_types

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

class CTD (Service):
    """ """
    def __init__(self, context): 
        super(CTD, self).__init__("ctd", context)
        logger.debug ("Ensuring presence of CTD files: {0}".format (self.url))
        files = [
            'CTD_chem_gene_ixns.tsv','CTD_chemicals.tsv'
        ]
        for f in files:
            if os.path.exists (f):
                continue
            logger.debug ("  --downloading CTD component: {0}".format (f))
            url = "{0}/{1}".format (self.url, f)
            r = requests.get (url, stream=True)
            with open (f, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write (chunk)

        self.load_names()
        self.load_genes()

    def load_names(self):
        self.name_to_id = defaultdict(list)
        fname = os.path.join (os.path.dirname (__file__), 'CTD_chemicals.tsv') 
        with open(fname,'r') as inf:
            line = inf.readline()
            while line.startswith('#'):
                line = inf.readline()
            for line in inf:
                x = line.strip().split('\t')
                indexname = x[0]
                self.name_to_id[ indexname.lower() ].append(indexname)
                if len(x) > 7:
                    synonyms = x[7].split('|')
                    for synonym in synonyms:
                        self.name_to_id[ synonym.lower() ].append(indexname)

    def load_genes(self):
        self.drug_genes = defaultdict(list)
        fname = os.path.join (os.path.dirname (__file__), 'CTD_chem_gene_ixns.tsv') 
        with open(fname,'r') as inf:
            line = inf.readline()
            while line.startswith('#'):
                line = inf.readline()
            for line in inf:
                x = line.strip().split('\t')
                organismid = x[7]
                if organismid != '9606':
                    continue
                chemname = x[0]
                result = {}
                result['gene_id'] = 'NCBIGENE:{}'.format(x[4])
                if len(x) > 9:
                    result['actions'] = x[9].split('|')
                else:
                    result['actions'] = []
                if len(x) > 10:
                    result['publications'] = x[10].split('|')
                else:
                    result['publications'] = []
                self.drug_genes[chemname].append(result)

    def drugname_to_ctd(self, namenode):
        drugname = Text.un_curie(namenode.identifier).lower()
        identifiers = self.name_to_id[ drugname ]
        results = []
        for ident in identifiers:
            ctdid = 'CTD:{}'.format(ident)
            label = drugname
            newnode = KNode( ctdid, node_types.DRUG, label=label)
            newedge = KEdge( 'CTD', 'drugname_to_ctd', {})
            results.append( (newedge, newnode ) )
        return results

    def drug_to_gene(self, subject):
        """ Get a gene from a ctd drug id. """
        ctdid = Text.un_curie (subject.identifier)
        actions = set()
        edge_nodes=[]
        for link in self.drug_genes[ctdid]:
            target_id = link['gene_id']
            edge_properties = { 'actions' : link['actions'],
                                'publications': link ['publications'] }
            actions.update( link['actions'] )
            edge = KEdge( 'ctd', 'drug_get_gene', {'properties': edge_properties } )
            node = KNode (target_id, node_types.GENE)
            edge_nodes.append( (edge, node) )
        for action in actions:
            print( 'Action: {}'.format(action) )
        return edge_nodes



def test_all_drugs():
    from greent.service import ServiceContext
    ctd = CTD(ServiceContext.create_context())
    with open('q2-drugandcondition-list.txt','r') as inf:
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
            drug_node = results [0][1]
            ident = drug_node.identifier
        except:
            n_no_ctd += 1
            ident=''
            gene_nodes=[]
        if ident != '':
            gene_nodes = ctd.drug_to_gene( drug_node )
            if len(gene_nodes) == 0:
                n_no_gene += 1
        print('{}\t{}\t{}\t{}'.format(name, ident, len(results), len(gene_nodes) ))
    print( '{} drugs'.format(len(uniq)) )
    print( '{} without pubchem id'.format(n_no_ctd) )
    print( '{} without genes'.format(n_no_gene) )
    ngood = len(uniq) - n_no_ctd - n_no_gene
    print( '{} good ({})'.format( ngood, ngood/len(uniq) ) )

if __name__ == "__main__":
    test_all_drugs()
