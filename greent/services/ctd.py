import logging
import requests
from datetime import datetime as dt
from greent.service import Service
from greent.graph_components import KNode, KEdge, LabeledID
from greent.util import Text, LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class CTD(Service):
    """ Interface to the Comparative Toxicogenomic Database data set."""
    def __init__(self, context):
        super(CTD, self).__init__("ctd", context)
        self.g2d_strings = ['abundance','response to substance','transport']
        self.term_parents = {'abundance': 'abundance',
                            'mutagenesis': 'mutagenesis',
                            'folding': 'folding',
                            'metabolic process': 'metabolic process',
                            'protein glutathionylation': 'metabolic process',
                            'glutathionylation': 'metabolic process',
                            'phosphorylation': 'metabolic process',
                            'protein ubiquitination': 'metabolic process',
                            'ubiquitination': 'metabolic process',
                            'protein sumoylation': 'metabolic process',
                            'sumoylation': 'metabolic process',
                            'protein nitrosylation': 'metabolic process',
                            'nitrosylation': 'metabolic process',
                            'protein nucleotidylation': 'metabolic process',
                            'nucleotidylation': 'metabolic process',
                            'carboxylation': 'metabolic process',
                            'methylation': 'metabolic process',
                            'protein carbamoylation': 'metabolic process',
                            'carbamoylation': 'metabolic process',
                            'sulfation': 'metabolic process',
                            'glycosylation': 'metabolic process',
                            'protein glucuronidation': 'metabolic process',
                            'glucuronidation': 'metabolic process',
                            'protein N-linked glycosylation': 'metabolic process',
                            'N-linked glycosylation': 'metabolic process',
                            'protein O-linked glycosylation': 'metabolic process',
                            'O-linked glycosylation': 'metabolic process',
                            'acylation': 'metabolic process',
                            'alkylation': 'metabolic process',
                            'amination': 'metabolic process',
                            'acetylation': 'metabolic process',
                            'glycation': 'metabolic process',
                            'hydroxylation': 'metabolic process',
                            'chemical synthesis': 'metabolic process',
                            'ethylation': 'metabolic process',
                            'oxidation': 'metabolic process',
                            'reduction': 'metabolic process',
                            'ribosylation': 'metabolic process',
                            'ADP-ribosylation': 'metabolic process',
                            'protein lipidation': 'metabolic process',
                            'lipidation': 'metabolic process',
                            'geranoylation': 'metabolic process',
                            'protein prenylation': 'metabolic process',
                            'prenylation': 'metabolic process',
                            'protein farnesylation': 'metabolic process',
                            'farnesylation': 'metabolic process',
                            'protein palmitoylation': 'metabolic process',
                            'palmitoylation': 'metabolic process',
                            'protein myristoylation': 'metabolic process',
                            'myristoylation': 'metabolic process',
                            'catabolic process': 'metabolic process',
                            'cleavage': 'metabolic process',
                            'hydrolysis': 'metabolic process',
                            'metabolic processing': 'metabolic process',
                            'glucuronidation': 'metabolic process',
                            'glutathionylation': 'metabolic process',
                            'degradation': 'metabolic process',
                            'nitrosation': 'metabolic process',
                            'transport': 'transport',
                            'secretion': 'secretion', #most transport is gene->drug, but secretion is usually drug->gene
                            'export': 'transport',
                            'uptake': 'transport',
                            'import': 'transport',
                            'reaction': 'reaction',
                            'stability': 'stability',
                            'cotreatment': 'cotreatment',
                            'activity': 'activity',
                            'binding': 'binding',
                            'RNA splicing': 'splicing',
                            'splicing': 'splicing',
                            'expression': 'expression',
                            'gene expression': 'expression',
                            'response to substance': 'response to substance',
                            'localization': 'localization' }

    def drugname_string_to_drug_identifier(self,drugname):
        #First, check to see if the name is already an exact name of something
        chemnamerows = requests.get (f"{self.url}CTD_chemicals_ChemicalName/{drugname}/").json ()
        keepers = [ x for x in chemnamerows if x['ChemicalName'].upper() == drugname.upper()]
        if len(keepers) == 0:
            #Didn't find exact name match, so now see if there is an exact synonym
            synonamerows = requests.get (f"{self.url}CTD_chemicals_Synonyms/{drugname}/").json ()
            for row in synonamerows:
                synonyms = [syn.upper() for syn in row['Synonyms'].split('|')]
                if drugname.upper() in synonyms:
                    keepers.append(row)
        return [ f"{r['ChemicalID']}" for r in keepers ]

    def drugname_string_to_drug(self, drugname):
        identifiers = self.drugname_string_to_drug_identifier(drugname)
        return [ KNode(identifier, node_types.DRUG) for identifier in identifiers ]

    def standardize_predicate(self, predicate):
        """CTD has a little more work to do than the standard service."""
        if '|' not in predicate.label:
            return self.concept_model.standardize_relationship(predicate)
        parts = predicate.label.split('|')
        goodparts = list(filter(lambda p:'reaction' not in p and 'cotreatment' not in p, parts))
        if len(goodparts) != 1:
            return self.concept_model.standardize_relationship(LabeledID('CTD:interacts_with','interacts_with'))
        #Change the modifier to "affects" to deal with the fact that we don't know what the deleted part does.
        thing = self.term_parents[goodparts[0].split('^')[1]]
        new_id = f'CTD:affects^{thing}'
        return self.concept_model.standardize_relationship(LabeledID(identifier=new_id,label=new_id))

    def get_ctd_predicate_identifier(self,label):
        chunk = label.split('|')
        breakups = [c.split('^') for c in chunk]
        renamed = [ f'{b[0]}^{self.term_parents[b[1]]}' for b in breakups]
        return f'CTD:{"|".join(renamed)}'

    def drug_to_gene(self, drug):
        output = []
        identifiers = drug.get_synonyms_by_prefix('MESH')
        for identifier in identifiers:
            url=f"{self.url}/CTD_chem_gene_ixns_ChemicalID/{Text.un_curie(identifier)}/"
            obj = requests.get(url).json ()
            for r in obj:
                props = {"description": r[ 'Interaction' ]}
                predicate_label = r['InteractionActions']
                predicate = LabeledID(self.get_ctd_predicate_identifier(predicate_label),predicate_label)
                gene_node = KNode(f"NCBIGENE:{r['GeneID']}", node_types.GENE)
                if sum([s in predicate.identifier for s in self.g2d_strings]) > 0:
                    subject = gene_node
                    object = drug
                else:
                    subject = drug
                    object = gene_node
                edge = self.create_edge(subject,object,'ctd.drug_to_gene',identifier,predicate,
                                        publications=[f"PMID:{r['PubMedIDs']}"],url=url,properties=props)
                output.append( (edge,gene_node) )
        return output

    def gene_to_drug(self, gene_node):
        output = []
        identifiers = gene_node.get_synonyms_by_prefix('NCBIGENE')
        for identifier in identifiers:
            url = f"{self.url}/CTD_chem_gene_ixns_GeneID/{Text.un_curie(identifier)}/"
            obj = requests.get (url).json ()
            for r in obj:
                props = {"description": r[ 'Interaction' ]}
                predicate_label = r['InteractionActions']
                predicate = LabeledID(self.get_ctd_predicate_identifier(predicate_label),predicate_label)
                #Should this be substance?
                drug_node = KNode(f"MESH:{r['ChemicalID']}", node_types.DRUG)
                if sum([s in predicate.identifier for s in self.g2d_strings]) > 0:
                    subject = gene_node
                    obj = drug_node
                else:
                    subject = drug_node
                    obj = gene_node
                edge = self.create_edge(subject,obj,'ctd.gene_to_drug',identifier,predicate,
                                        publications=[f"PMID:{r['PubMedIDs']}"],url=url,properties=props)
                output.append( (edge,drug_node) )
        return output

