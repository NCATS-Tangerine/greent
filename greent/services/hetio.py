import json
import unittest
from pprint import pprint
from greent.neo4jbase import Neo4JREST
from greent.util import Munge
from greent.util import Text
from greent.graph_components import KNode, LabeledID
from greent import node_types
from datetime import datetime as dt

"""
Notes: 
 Hetio contains:
  anatomy terms that use an UBERON curie as the "identifier" property
  genes that use the Entrez ID (NCBIGENE) number (not curie and not string) as the "identifier" property
  diseases use a DOID curie as the "identifier" property
  phenotypes are in there as symptoms.  The identifier property is a Mesh ID (not a curie)
"""

class HetIO(Neo4JREST):

    def __init__(self, context): #url="https://neo4j.het.io"):
        super (HetIO, self).__init__("hetio", context)

    def munge_gene (self, gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene

    def gene_to_anatomy (self, gene):
        gene_identifiers = list(gene.get_synonyms_by_prefix('NCBIGENE'))
        gene_identifier = Text.un_curie(gene_identifiers[0])
        nodes,edges = self.query ( "MATCH (a:Anatomy)-[ar]-(g:Gene) WHERE g.identifier={0} RETURN a, ar, g LIMIT 200".format (gene_identifier),
                              labels=['Anatomy'], kinds=['node','relationship'])
        node_ids = [ LabeledID(identifier=node['identifier'], label=node['name']) for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate = LabeledID(identifier=f'hetio:{predicate_label}', label=predicate_label)
            anatomy = KNode(node_id.identifier, type=node_types.ANATOMY, name=node_id.label)
            #These edges all go from anatomy to gene
            edge = self.create_edge(anatomy, gene,'hetio.gene_to_anatomy',gene_identifier,predicate)
            results.append((edge, anatomy))
        return results

    def anatomy_to_gene (self, anat):
        anat_identifiers = list(anat.get_synonyms_by_prefix('UBERON'))
        anat_identifier = anat_identifiers[0]
        nodes,edges = self.query ( "MATCH (a:Anatomy)-[ar]-(g:Gene) WHERE a.identifier='{0}' RETURN a, ar, g ".format (anat_identifier),
                              labels=['Gene'], kinds=['node','relationship'])
        node_ids = [ LabeledID(identifier=f"NCBIGENE:{node['identifier']}", label=node['name']) for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate = LabeledID(identifier=f'hetio:{predicate_label}', label=predicate_label)
            gene = KNode(node_id.identifier, type=node_types.GENE, name=node_id.label)
            #These edges all go from anatomy to gene
            edge = self.create_edge(anat, gene,'hetio.anatomy_to_gene',anat_identifier,predicate)
            results.append((edge, gene))
        return results

    #TODO: this is not to a cell, but a cellular component.  REmoving it from the yaml until we can fix it up
    def gene_to_cellular_component (self, gene):
        result = self.query (
            "MATCH (g:Gene)-[r]-(c:CellularComponent) WHERE g.name='{0}' RETURN g, r, c LIMIT 200".format (Text.un_curie (gene.id)),
            labels=['CellularComponent'],
            node_properties=['identifier','name'])
        anatomies = []
        return [ ( self.get_edge ({ 'res' : r }, predicate='affects'), KNode(r['identifier'], type=node_types.CELLULAR_COMPONENT, name=r['name']) ) for r in result ]

    #TODO: implement the reverse too
    def gene_to_disease (self, gene):
        gene_identifiers = list(gene.get_synonyms_by_prefix('NCBIGENE'))
        if len(gene_identifiers) == 0:
            return []
        gene_identifier = Text.un_curie(gene_identifiers[0])
        nodes, edges = self.query (
            "MATCH (d:Disease)-[a1]-(g:Gene) WHERE g.identifier={0} RETURN a1,d".format (gene_identifier),
            labels=['Disease'], kinds=['node','relationship'])
        node_ids = [ LabeledID(identifier=node['identifier'], label=node['name']) for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        #These edges all go from disease to gene
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate = LabeledID(identifier=f'hetio:{predicate_label}', label=predicate_label)
            disease = KNode(node_id.identifier, type=node_types.DISEASE, name=node_id.label)
            edge = self.create_edge(disease, gene,'hetio.gene_to_disease',gene_identifier,predicate)
            results.append( (edge, disease) )
        return results

    def disease_to_phenotype (self, disease):
        disease_identifiers = list(disease.get_synonyms_by_prefix('DOID'))
        if len(disease_identifiers) == 0:
            return []
        disease_identifier = disease_identifiers[0]
        query = """MATCH (d:Disease{identifier:'%s'})-[r]-(s:Symptom) RETURN d,r,s""" % (disease_identifier)
        nodes,edges = self.query (query, labels=['Symptom'], kinds=['node','relationship'])
        node_ids = [ LabeledID(identifier=f"MESH:{node['identifier']}", label=node['name']) for node in nodes ]
        edge_ids = [ edge['type'] for edge in edges ]
        results = []
        for node_id, predicate_label in zip(node_ids,edge_ids):
            predicate = LabeledID(identifier=f'hetio:{predicate_label}', label=predicate_label)
            phenotype = KNode(node_id.identifier, type=node_types.PHENOTYPE, name=node_id.label)
            edge = self.create_edge(disease, phenotype, 'hetio.disease_to_phenotype', disease_identifier, predicate)
            results.append( (edge, phenotype) )
        return results

