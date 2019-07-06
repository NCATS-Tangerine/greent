import logging
import requests
from datetime import datetime as dt
from greent.service import Service
from greent.graph_components import KNode, LabeledID
from greent.util import Text, LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class gtopdb(Service):
    """ Interface to the Guide to Pharmacology."""
    def __init__(self, context):
        super(gtopdb, self).__init__("gtopdb", context)

    def chem_to_precursor(self, drug):
        output = []
        identifiers = drug.get_synonyms_by_prefix('GTOPDB')
        for identifier in identifiers:
            ligandId = Text.un_curie(identifier)
            url=f"{self.url}/ligands/{ligandId}/precursors"
            obj = requests.get(url).json ()
            for r in obj:
                if r['species'] != 'Human':
                    continue
                gene_node = KNode(f"HGNC:{r['officialGeneId']}", type=node_types.GENE)
                predicate = LabeledID(identifier='RO:0002205', label='has gene product')
                edge = self.create_edge(gene_node,drug,'gtopdb.chem_to_precursor',identifier,predicate)
                output.append( (edge,gene_node) )
        return output

    def ligand_to_gene(self, drug):
        output = []
        identifiers = drug.get_synonyms_by_prefix('GTOPDB')
        for identifier in identifiers:
            ligandId = Text.un_curie(identifier)
            url=f"{self.url}/ligands/{ligandId}/interactions"
            obj = requests.get(url).json ()
            for r in obj:
                if r['species'] != 'Human':
                    continue
                gene_node = KNode(f"IUPHAR:{r['targetId']}", type=node_types.GENE)
                'Activator', 'Agonist', 'Allosteric modulator', 'Antagonist', 'Antibody', 'Channel blocker', 'Gating inhibitor', 'Inhibitor', 'Subunit-specific'
                if r['type'] == 'Agonist':
                    predicate = LabeledID(identifier='CTD:increases_activity_of', label='increases activity of')
                elif r['type'] in ['Antagonist','Channel blocker', 'Inhibitor', 'Gating inhibitor']:
                    predicate = LabeledID(identifier='CTD:decreases_activity_of', label='decreases activity of')
                else:
                    predicate = LabeledID(identifier='RO:0002434', label='interacts with')
                edge = self.create_edge(drug,gene_node,'gtopdb.ligand_to_gene',identifier,predicate,
                    publications=[f"PMID:{x}" for x in r['PubMedIDs'].split('|')],url=url,properties=props)
                output.append( (edge,gene_node) )
        return output

