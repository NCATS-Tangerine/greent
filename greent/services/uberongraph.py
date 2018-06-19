from string import Template
import json
import os
import logging
from greent.service import Service
from greent.triplestore import TripleStore
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KEdge, KNode, LabeledID
from greent import node_types
from pprint import pprint
from datetime import datetime as dt
import datetime

logger = LoggingUtil.init_logging(__name__)

class UberonGraphKS(Service):
    """A knowledge source created by 1) Combining cell ontology, uberon, and
    HPO, 2) Reasoning over the total graph to realize many implicit edges.
    Created by Jim Balhoff"""

    def __init__(self, context): #triplestore):
        super(UberonGraphKS, self).__init__("uberongraph", context)
        self.triplestore = TripleStore (self.url)

    def query_uberongraph (self, query):
        """ Execute and return the result of a SPARQL query. """
        return self.triplestore.execute_query (query)


    def cell_get_cellname (self, cell_identifier):
        """ Identify label for a cell type
        :param cell: CL identifier for cell type 
        """
        text = """
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        select distinct ?cellLabel
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
                  $cellID rdfs:label ?cellLabel .
              }
        """
        results = self.triplestore.query_template( 
            inputs = { 'cellID': cell_identifier }, \
            outputs = [ 'cellLabel' ], \
            template_text = text \
        )
        return results


    def get_anatomy_parts(self, anatomy_identifier):
        """Given an UBERON id, find other UBERONS that are parts of the query"""
        if anatomy_identifier.startswith('http'):
            anatomy_identifier = Text.obo_to_curie(anatomy_identifier)
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix BFO: <http://purl.obolibrary.org/obo/BFO_>
        select distinct ?part ?partlabel
        from <http://reasoner.renci.org/nonredundant> 
        from <http://example.org/uberon-hp-cl.ttl>
        where {
                $anatomy_id BFO:0000051 ?part .
                graph <http://reasoner.renci.org/redundant> {
                  ?part rdfs:subClassOf UBERON:0001062 .
                }
                ?part rdfs:label ?partlabel .
        }
        """
        results = self.triplestore.query_template(  
            inputs  = { 'anatomy_id': anatomy_identifier }, \
            outputs = [ 'part', 'partlabel' ], \
            template_text = text \
        )
        for result in results:
            result['curie'] = Text.obo_to_curie(result['part'])
        return results


    def anatomy_to_cell (self, anatomy_identifier):
        """ Identify anatomy terms related to cells.

        :param cell: CL identifier for cell type
        """
        text = """
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix BFO: <http://purl.obolibrary.org/obo/BFO_>
        select distinct ?cellID ?cellLabel
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
            graph <http://reasoner.renci.org/redundant> {
                ?cellID rdfs:subClassOf CL:0000000 .
                ?cellID BFO:0000050 $anatomyID .
            }
            ?cellID rdfs:label ?cellLabel .
        }

        """
        results = self.triplestore.query_template(
            inputs = { 'anatomyID': anatomy_identifier }, \
            outputs = [ 'cellID', 'cellLabel' ], \
            template_text = text \
        )
        return results


    def cell_to_anatomy (self, cell_identifier):
        """ Identify anatomy terms related to cells.

        :param cell: CL identifier for cell type 
        """
        text = """
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix BFO: <http://purl.obolibrary.org/obo/BFO_>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        select distinct ?anatomyID ?anatomyLabel
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
            graph <http://reasoner.renci.org/redundant> {
                ?anatomyID rdfs:subClassOf UBERON:0001062 .
                $cellID BFO:0000050 ?anatomyID .
            }
            ?anatomyID rdfs:label ?anatomyLabel .
        }
        """
        results = self.triplestore.query_template( 
            inputs = { 'cellID': cell_identifier }, \
            outputs = [ 'anatomyID', 'anatomyLabel' ], \
            template_text = text \
        )
        return results

    def phenotype_to_anatomy (self, hp_identifier):
        """ Identify anatomy terms related to cells.

        :param cell: HP identifier for phenotype
        """

        #The subclassof uberon:0001062 ensures that the result
        #is an anatomical entity.
        text = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix part_of: <http://purl.obolibrary.org/obo/BFO_0000050>
        prefix has_part: <http://purl.obolibrary.org/obo/BFO_0000051>
        prefix depends_on: <http://purl.obolibrary.org/obo/RO_0002502>
        prefix phenotype_of: <http://purl.obolibrary.org/obo/UPHENO_0000001>
        select distinct ?anatomy_id ?anatomy_label ?input_label
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
                  graph <http://reasoner.renci.org/redundant> {
                    ?anatomy_id rdfs:subClassOf UBERON:0001062 .
                  }
                  ?anatomy_id rdfs:label ?anatomy_label .
                  graph <http://reasoner.renci.org/nonredundant> {
                       ?phenotype phenotype_of: ?anatomy_id .
                  }
                  graph <http://reasoner.renci.org/redundant> {
                    $HPID rdfs:subClassOf ?phenotype .
                  }
                  $HPID rdfs:label ?input_label .
              }
        """
        results = self.triplestore.query_template( 
            inputs = { 'HPID': hp_identifier }, \
            outputs = [ 'anatomy_id', 'anatomy_label', 'input_label'],\
            template_text = text \
        )
        return results

    def anatomy_to_phenotype(self, uberon_id):
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix part_of: <http://purl.obolibrary.org/obo/BFO_0000050>
        prefix has_part: <http://purl.obolibrary.org/obo/BFO_0000051>
        prefix depends_on: <http://purl.obolibrary.org/obo/RO_0002502>
        prefix phenotype_of: <http://purl.obolibrary.org/obo/UPHENO_0000001>
        select distinct ?pheno_id ?anatomy_label ?pheno_label
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
                  $UBERONID rdfs:label ?anatomy_label .
                  graph <http://reasoner.renci.org/nonredundant> {
                       ?phenotype phenotype_of: $UBERONID .
                  }
                  graph <http://reasoner.renci.org/redundant> {
                    ?pheno_id rdfs:subClassOf ?phenotype .
                  }
                  ?pheno_id rdfs:label ?pheno_label .
              }
        """
        #The subclassof uberon:0001062 ensures that the result
        #is an anatomical entity.
        results = self.triplestore.query_template(
            inputs = { 'UBERONID': uberon_id }, \
            outputs = [ 'pheno_id', 'anatomy_label', 'pheno_label'],\
            template_text = text \
        )
        return results


    def get_anatomy_by_cell_graph (self, cell_node):
        anatomies = self.cell_to_anatomy (cell_node.identifier)
        results = []
        predicate = LabeledID('BFO:0000050', 'part_of')
        for r in anatomies:
            anatomy_node = KNode (Text.obo_to_curie(r['anatomyID']), node_types.ANATOMY, label=r['anatomyLabel'] )
            edge = self.create_edge(cell_node, anatomy_node, 'uberongraph.get_anatomy_by_cell_graph', cell_node.identifier, predicate)
            results.append ( (edge, anatomy_node) )
        return results

    def get_cell_by_anatomy_graph (self, anatomy_node):
        cells = self.anatomy_to_cell(anatomy_node.identifier)
        results = []
        predicate = LabeledID('BFO:0000050', 'part_of')
        for r in cells:
            cell_node = KNode (Text.obo_to_curie(r['cellID']), node_types.CELL, label=r['cellLabel'] )
            edge = self.create_edge(cell_node, anatomy_node, 'uberongraph.get_cell_by_anatomy_graph', anatomy_node.identifier, predicate)
            results.append ( (edge, cell_node) )
        return results

    def create_phenotype_anatomy_edge(self, node_id, node_label, input_id ,phenotype_node):
        predicate = LabeledID('GAMMA:0000002','inverse of has phenotype affecting')
        anatomy_node = KNode ( Text.obo_to_curie(node_id), node_types.ANATOMY , label=node_label)
        edge = self.create_edge(anatomy_node, phenotype_node,'uberongraph.get_anatomy_by_phenotype_graph', input_id, predicate)
        #node.label = node_label
        return edge,anatomy_node

    def create_anatomy_phenotype_edge(self, node_id, node_label, input_id ,anatomy_node):
        predicate = LabeledID('GAMMA:0000002','inverse of has phenotype affecting')
        phenotype_node = KNode ( Text.obo_to_curie(node_id), node_types.PHENOTYPE , label=node_label)
        edge = self.create_edge(anatomy_node, phenotype_node,'uberongraph.get_phenotype_by_anatomy_graph', input_id, predicate)
        #node.label = node_label
        return edge,phenotype_node

    def get_anatomy_by_phenotype_graph (self, phenotype_node):
        results = []
        for curie in phenotype_node.get_synonyms_by_prefix('HP'):
            anatomies = self.phenotype_to_anatomy (curie)
            for r in anatomies:
                edge, node = self.create_phenotype_anatomy_edge(r['anatomy_id'],r['anatomy_label'],curie,phenotype_node)
                if phenotype_node.label is None:
                    phenotype_node.label = r['input_label']
                results.append ( (edge, node) )
                #These tend to be very high level terms.  Let's also get their parts to
                #be more inclusive.
                #TODO: there ought to be a more principled way to take care of this, but
                #it highlights the uneasy relationship between the high level world of
                #smartapi and the low-level sparql-vision.
                part_results = self.get_anatomy_parts( r['anatomy_id'] )
                for pr in part_results:
                    pedge, pnode = self.create_phenotype_anatomy_edge(pr['part'],pr['partlabel'],curie,phenotype_node)
                    results.append ( (pedge, pnode) )
        return results

    def get_phenotype_by_anatomy_graph (self, anatomy_node):
        results = []
        for curie in anatomy_node.get_synonyms_by_prefix('UBERON'):
            phenotypes = self.anatomy_to_phenotype (curie)
            for r in phenotypes:
                edge, node = self.create_anatomy_phenotype_edge(r['pheno_id'],r['pheno_label'],curie,anatomy_node)
                if anatomy_node.label is None:
                    anatomy_node.label = r['anatomy_label']
                results.append ( (edge, node) )
        return results
