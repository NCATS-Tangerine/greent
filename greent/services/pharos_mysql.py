from greent.util import Text
from greent.service import Service
from greent.util import LoggingUtil
from greent.graph_components import KEdge, KNode, LabeledID
from greent import node_types
import logging
import mysql.connector

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class PharosMySQL(Service):
    def __init__(self, context):
        super(PharosMySQL, self).__init__("pharos_mysql", context)
        self.db  = mysql.connector.connect(user='tcrd', host=self.url, database='tcrd520', buffered = True)

    def gene_get_disease(self, gene_node):
        identifiers = gene_node.get_synonyms_by_prefix('HGNC')
        predicate = LabeledID(identifier='PHAROS:gene_involved', label='gene_involved')
        resolved_edge_nodes = []
        for hgnc in identifiers:
            query = f"select distinct d.did,d.name from disease d join xref x on x.protein_id = d.target_id where x.xtype = 'HGNC' and d.dtype <> 'Expression Atlas' and x.value = '{hgnc}'"
            cursor = self.db.cursor(dictionary = True, buffered = True)
            cursor.execute(query)
            for result in cursor:
                did = result['did']
                label = result['name']
                disease_node = KNode(did, type=node_types.DISEASE, name=label)
                edge = self.create_edge(disease_node,gene_node, 'pharos.gene_get_disease',hgnc,predicate)
                resolved_edge_nodes.append( (edge,disease_node) )
        return resolved_edge_nodes
 

    def g2d(self,hgnc,query,chembls,resolved_edge_nodes,gene_node):
        predicate = LabeledID(identifier='PHAROS:drug_targets', label='is_target')
        cursor = self.db.cursor(dictionary = True, buffered = True)
        cursor.execute(query)
        for result in cursor:
            label = result['drug']
            chemblid = f"CHEMBL:{result['cmpd_chemblid']}"
            if chemblid not in chembls:
                chembls.add(chemblid)
                drug_node = KNode(chemblid, type=node_types.CHEMICAL_SUBSTANCE, name=label)
                edge = self.create_edge(drug_node,gene_node, 'pharos.gene_get_drug',hgnc,predicate)
                resolved_edge_nodes.append( (edge,drug_node) )

    def gene_get_drug(self, gene_node):
        """ Get a drug from a gene. """
        resolved_edge_nodes = []
        identifiers = gene_node.get_synonyms_by_prefix('HGNC')
        chembls = set()
        for hgnc in identifiers:
            query1=f"select distinct da.drug, da.cmpd_chemblid from xref x, drug_activity da  where  x.protein_id = da.target_id and x.xtype='HGNC' and x.value = '{hgnc}';"
            self.g2d(hgnc,query1,chembls,resolved_edge_nodes,gene_node)
            query2=f"select distinct da.cmpd_name_in_ref as drug, da.cmpd_chemblid from xref x, chembl_activity da  where  x.protein_id = da.target_id and x.xtype='HGNC' and x.value = '{hgnc}';"
            self.g2d(hgnc,query2,chembls,resolved_edge_nodes,gene_node)
        return resolved_edge_nodes

    def d2g(self, drug_node, query, resolved_edge_nodes, chembl,hgncs):
        """ Get a gene from a drug. """
        predicate = LabeledID(identifier='PHAROS:drug_targets', label='is_target')
        cursor = self.db.cursor(dictionary = True, buffered = True)
        cursor.execute(query)
        for result in cursor:
            label = result['sym']
            hgnc = result['value']
            if hgnc not in hgncs:
                hgncs.add(hgnc)
                gene_node = KNode(hgnc, type=node_types.GENE, name=label)
                edge = self.create_edge(drug_node,gene_node, 'pharos.drug_get_gene',chembl,predicate)
                resolved_edge_nodes.append( (edge,gene_node) )

    def drug_get_gene(self, drug_node):
        """ Get a gene from a drug. """
        resolved_edge_nodes = []
        identifiers = drug_node.get_synonyms_by_prefix('CHEMBL')
        predicate = LabeledID(identifier='PHAROS:drug_targets', label='is_target')
        hgncs = set()
        for chembl in identifiers:
            query=f"select distinct x.value, p.sym from xref x, drug_activity da, protein p where da.target_id = x.protein_id and da.cmpd_chemblid='{Text.un_curie(chembl)}' and x.xtype='HGNC' and da.target_id = p.id;"
            self.d2g(drug_node, query, resolved_edge_nodes, chembl,hgncs)
            query=f"select distinct x.value, p.sym from xref x, chembl_activity da, protein p where da.target_id = x.protein_id and da.cmpd_chemblid='{Text.un_curie(chembl)}' and x.xtype='HGNC' and da.target_id = p.id;"
            self.d2g(drug_node, query, resolved_edge_nodes, chembl,hgncs)
        return resolved_edge_nodes


#select distinct x.value  from disease d join xref x on x.protein_id = d.target_id where x.xtype = 'HGNC' and d.did='DOID:5572' order by x.value;
    def disease_get_gene(self, disease_node):
        """ Get a gene from a pharos disease id."""
        resolved_edge_nodes = []
        hgncs = set()
        predicate = LabeledID(identifier='PHAROS:gene_involved', label='gene_involved')
        #Pharos contains multiple kinds of disease identifiers in its disease table:
        # For OMIM identifiers, they can have either prefix OMIM or MIM
        # UMLS doen't have any prefixes.... :(
        pharos_predicates = {'DOID':('DOID',),'UMLS':(None,),'MESH':('MESH',),'OMIM':('OMIM','MIM'),'ORPHANET':('Orphanet',)}
        for ppred,dbpreds in pharos_predicates.items():
            pharos_candidates = [Text.un_curie(x) for x in disease_node.get_synonyms_by_prefix(ppred)]
            for dbpred in dbpreds:
                if dbpred is None:
                    pharos_ids = pharos_candidates
                else:
                    pharos_ids = [f'{dbpred}:{x}' for x in pharos_candidates]
                    for pharos_id in pharos_ids:
                        cursor = self.db.cursor(dictionary = True, buffered = True)
                        query = f"select distinct x.value, p.sym  from disease d join xref x on x.protein_id = d.target_id join protein p on d.target_id = p.id where x.xtype = 'HGNC' and d.dtype <> 'Expression Atlas' and d.did='{pharos_id}';"
                        cursor.execute(query)
                        for result in cursor:
                            label = result['sym']
                            hgnc = result['value']
                            if hgnc not in hgncs:
                                hgncs.add(hgnc)
                                gene_node = KNode(hgnc, type=node_types.GENE, name=label)
                                edge = self.create_edge(disease_node,gene_node, 'pharos.disease_get_gene',pharos_id,predicate)
                                resolved_edge_nodes.append( (edge,gene_node) )
        return resolved_edge_nodes
