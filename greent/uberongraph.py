from string import Template
import json
import os
import logging
from greent.service import Service
from greent.service import ServiceContext
from greent.triplestore import TripleStore
from greent.util import LoggingUtil
from greent.util import Text
from reasoner.graph_components import KEdge, KNode
from reasoner import node_types
from pprint import pprint
from cachier import cachier
import datetime

logger = LoggingUtil.init_logging (__file__)

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

    def cell_to_anatomy (self, cell_identifier):
        """ Identify anatomy terms related to cells.

        :param cell: CL identifier for cell type 
        """
        text = """
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix BFO: <http://purl.obolibrary.org/obo/BFO_>
        select distinct ?anatomyID ?anatomyLabel
        from <http://reasoner.renci.org/nonredundant>
        from <http://example.org/uberon-hp-cl.ttl>
        where {
                  $cellID rdfs:subClassOf*/BFO:0000050 ?anatomyID .
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
                  ?anatomy_id rdfs:subClassOf* UBERON:0001062.
                  ?anatomy_id rdfs:label ?anatomy_label .
                  graph <http://reasoner.renci.org/nonredundant> {
                       ?phenotype phenotype_of: ?anatomy_id .
                  }
                  $HPID rdfs:subClassOf* ?phenotype .
                  $HPID rdfs:label ?input_label .
              }
        """
        results = self.triplestore.query_template( 
            inputs = { 'HPID': hp_identifier }, \
            outputs = [ 'anatomy_id', 'anatomy_label', 'input_label'],\
            template_text = text \
        )
        return results


    def get_anatomy_by_cell_graph (self, cell_node):
        anatomies = self.cell_to_anatomy (cell_node.identifier)
        results = []
        for r in anatomies:
            edge = KEdge ('uberongraph', 'cellToAnatomy')
            node = KNode (Text.obo_to_curie(r['anatomyID']), \
                   node_types.ANATOMY )
            node.label = r['anatomyLabel']
            results.append ( (edge, node) )
        return results
    
    def get_anatomy_by_phenotype_graph (self, phenotype_node):
        anatomies = self.phenotype_to_anatomy (phenotype_node.identifier)
        results = []
        for r in anatomies:
            edge = KEdge ('uberongraph', 'phenotypeToAnatomy')
            node = KNode ( Text.obo_to_curie(r['anatomy_id']), \
                   node_types.ANATOMY )
            node.label = r['anatomy_label']
            if phenotype_node.label is None:
                phenotype_node.label = r['input_label']
            results.append ( (edge, node) )
        return results

def test():
    uk = UberonGraphKS(ServiceContext.create_context ())
    #Test cell->anatomy
    k = KNode('CL:0000097',node_types.CELL)
    results = uk.get_anatomy_by_cell_graph( k )
    print(results)
    #Test pheno->anatomy
    k = KNode('HP:0000403',node_types.PHENOTYPE)
    results = uk.get_anatomy_by_phenotype_graph( k )
    print(results)

if __name__ == '__main__':
    test()
