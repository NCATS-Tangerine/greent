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
            query = f"select distinct d.did,d.name from disease d join xref x on x.protein_id = d.target_id where x.xtype = 'HGNC' and x.value = '{hgnc}'"
            cursor = self.db.cursor(dictionary = True, buffered = True)
            cursor.execute(query)
            for result in cursor:
                print(result)
                did = result['did']
                label = result['name']
                disease_node = KNode(did, type=node_types.DISEASE, name=label)
                edge = self.create_edge(disease_node,gene_node, 'pharos.gene_get_disease',hgnc,predicate)
                resolved_edge_nodes.append( (edge,disease_node) )
        return resolved_edge_nodes
 

    def g2d(self,hgnc,query,chembls,resolved_edge_nodes):
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
        predicate = LabeledID(identifier='PHAROS:drug_targets', label='is_target')
        chembls = set()
        for hgnc in identifiers:
            query1=f"select distinct da.drug, da.cmpd_chemblid from xref x, drug_activity da  where  x.protein_id = da.target_id and x.xtype='HGNC' and x.value = '{hgnc}';"
            self.g2d(hgnc,query1,chembls,resolved_edge_nodes)
            query2=f"select distinct da.cmpd_name_in_ref as drug, da.cmpd_chemblid from xref x, chembl_activity da  where  x.protein_id = da.target_id and x.xtype='HGNC' and x.value = '{hgnc}';"
            self.g2d(hgnc,query2,chembls,resolved_edge_nodes)
        return resolved_edge_nodes

#select distinct d.drug_name, da.cmpd_chemblid from disease d join drug_activity da on d.drug_name=da.drug  where d.name='Asthma' and d.drug_name is not NULL order by d.drug_name;

    def drug_get_gene(self, drug_node):
        """ Get a gene from a drug. """
        resolved_edge_nodes = []
        identifiers = drug_node.get_synonyms_by_prefix('CHEMBL')
        predicate = LabeledID(identifier='PHAROS:drug_targets', label='is_target')
        for chembl in identifiers:
            query=f"select distinct x.value, p.sym from xref x, drug_activity da, protein p where da.target_id = x.protein_id and da.cmpd_chemblid='{Text.un_curie(chembl)}' and x.xtype='HGNC' and da.target_id = p.id;"
            print(query)
            cursor = self.db.cursor(dictionary = True, buffered = True)
            cursor.execute(query)
            for result in cursor:
                print(result)
                label = result['sym']
                hgnc = result['value']
                gene_node = KNode(hgnc, type=node_types.GENE, name=label)
                edge = self.create_edge(drug_node,gene_node, 'pharos.drug_get_gene',chembl,predicate)
                resolved_edge_nodes.append( (edge,gene_node) )
        return resolved_edge_nodes



""" 
#select distinct x.value  from disease d join xref x on x.protein_id = d.target_id where x.xtype = 'HGNC' and d.did='DOID:5572' order by x.value;
    def disease_get_gene(self, subject):
        "" Get a gene from a pharos disease id. ""
        pharos_ids = subject.get_synonyms_by_prefix('DOID')
        resolved_edge_nodes = []
        for pharosid in pharos_ids:
            logging.getLogger('application').debug("Identifier:" + subject.id)
            original_edge_nodes = []
            url='https://pharos.nih.gov/idg/api/v1/diseases/%s?view=full' % pharosid
            logger.info(url)
            r = requests.get(url)
            result = r.json()
            predicate=LabeledID(identifier='PHAROS:gene_involved', label='gene_involved')
            for link in result['links']:
                if link['kind'] == 'ix.idg.models.Target':
                    pharos_target_id = int(link['refid'])
                    logger.info(f"Pharos ID: {pharos_target_id}")
                    hgnc = self.target_to_hgnc(pharos_target_id)
                    if hgnc is not None:
                        hgnc_node = KNode(hgnc, type=node_types.GENE)
                        edge = self.create_edge(subject,hgnc_node,'pharos.disease_get_gene',pharosid,predicate,url=url)
                        resolved_edge_nodes.append((edge, hgnc_node))
                        logger.info(f" HGNC ID: {hgnc}")
                    else:
                        logging.getLogger('application').warn('Did not get HGNC for pharosID %d' % pharos_target_id)
            return resolved_edge_nodes
"""
