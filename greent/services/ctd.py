import logging
import os
import requests
from collections import defaultdict
from greent.service import Service
import greent.util
from greent.graph_components import KNode, KEdge
from greent.util import Text
from greent import node_types

logger = greent.util.LoggingUtil.init_logging(__file__, level=logging.DEBUG)

class CTD(Service):
    """ Interface to the Comparative Toxicogenomic Database data set."""
    def __init__(self, context):
        super(CTD, self).__init__("ctd", context)
    def drugname_to_ctd(self, namenode):
        drugname = Text.un_curie(namenode.identifier)
        ctdids = self.drugname_string_to_ctd_string(drugname)
        results = []
        for ctdid in ctdids:
            label = drugname
            newnode = KNode(ctdid, node_types.DRUG, label=label)
            newedge = KEdge('CTD', 'drugname_to_ctd', {})
            results.append((newedge, newnode))
        return results
    def drugname_string_to_ctd_string(self, drugname):
        obj = requests.get (f"{self.url}/CTD_chem_gene_ixns_ChemicalName/{Text.un_curie(subject.identifier)}/").json ()
        return [
            ( self.get_edge(props=r, pmids=r['PubMedIDs']),
              KNode(f"MESH:{r['ChemicalID']}", "drug") ) for r in obj ]

    def drug_to_gene(self, subject):
        obj = requests.get (f"{self.url}/CTD_chem_gene_ixns_ChemicalID/{Text.un_curie(subject.identifier)}/").json ()
        return [
            ( self.get_edge(props=r, pmids=r['PubMedIDs']),
              KNode(f"NCBIGene:{r['GeneID']}", "gene") ) for r in obj ]

    def gene_to_drug(self, subject):
        obj = requests.get (f"{self.url}/CTD_chem_gene_ixns_GeneID/{Text.un_curie(subject.identifier)}/").json ()
        return [
            ( self.get_edge(props=r, pmids=r['PubMedIDs']),
              KNode(f"MESH:{r['ChemicalID']}", "drug") ) for r in obj ]

def test_d2g():
    from greent.service import ServiceContext
    ctd = CTD(ServiceContext.create_context())
    input_node = KNode("DRUGBANK:DB00482", node_types.DRUG)
    input_node.add_synonyms(set(["CTD:Celecoxib"]))
    results = ctd.drug_to_gene(input_node)
    print(results)
    
    input_node = KNode("MESH:D000068877", "disease")
    results = ctd.drug_to_gene(input_node)
    print(results)

def test_all_drugs():
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

if __name__ == "__main__":
    test_all_drugs()
    test_d2g()
