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
                if r['targetSpecies'] != 'Human':
                    continue
                gene_node = KNode(f"IUPHAR:{r['targetId']}", type=node_types.GENE)
                edge = self.make_edge(drug,gene_node,r,identifier,url)
                output.append( (edge,gene_node) )
        return output

    def gene_to_ligand(self, gene):
        output = []
        identifiers = gene.get_synonyms_by_prefix('IUPHAR')
        for identifier in identifiers:
            targetid = Text.un_curie(identifier)
            url=f"{self.url}/targets/{targetid}/interactions"
            obj = requests.get(url).json ()
            for r in obj:
                if r['species'] != 'Human':
                    continue
                chem_node = KNode(f"GTOPDB:{r['targetId']}", type=node_types.GENE)
                edge = self.make_edge(chem_node,gene,r,identifier,url)
                output.append( (edge,chem_node) )
        return output

    def make_edge(self,chem,gene,r,identifier,url):
        'Activator', 'Agonist', 'Allosteric modulator', 'Antagonist', 'Antibody', 'Channel blocker', 'Gating inhibitor', 'Inhibitor', 'Subunit-specific'
        if r['type'] == 'Agonist':
            predicate = LabeledID(identifier='CTD:increases_activity_of', label='increases activity of')
        elif r['type'] in ['Antagonist','Channel blocker', 'Inhibitor', 'Gating inhibitor']:
            predicate = LabeledID(identifier='CTD:decreases_activity_of', label='decreases activity of')
        else:
            predicate = LabeledID(identifier='RO:0002434', label='interacts with')
        props = {x: r[x] for x in ['primaryTarget', 'affinityParameter', 'endogenous'] }
        affins = [float(x.strip()) for x in r['affinity'].split('-') ]
        if len(affins) > 0:
            props['affinity'] = sum(affins) / len(affins)
        edge = self.create_edge(chem,gene,'gtopdb.ligand_to_gene',identifier,predicate,
            publications=[f"PMID:{x['pmid']}" for x in r['refs']],url=url,properties=props)
        return edge
