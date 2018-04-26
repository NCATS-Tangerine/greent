import logging
import requests
from datetime import datetime as dt
from greent.service import Service
from greent.graph_components import KNode, KEdge
from greent.util import Text, LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)

class CTD(Service):
    """ Interface to the Comparative Toxicogenomic Database data set."""
    def __init__(self, context):
        super(CTD, self).__init__("ctd", context)
        self.concept_model = getattr(context, 'rosetta-graph').concept_model
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
                            'secretion': 'transport',
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

    def standardize_predicate(self, p_id, p_label):
        if '|' not in p_label:
            return self.concept_model.standardize_relationship(p_id)
        parts = p_label.split('|')
        goodparts = list(filter(lambda p:'reaction' not in p and 'cotreatment' not in p, parts))
        if len(goodparts) != 1:
            return self.concept_model.standardize_relationship('CTD:interacts_with')
        #Change the modifier to "affects" to deal with the fact that we don't know what the deleted part does.
        thing = self.term_parents[goodparts[0].split('^')[1]]
        new_id = f'CTD:affects^{thing}'
        return self.concept_model.standardize_relationship(new_id)

    def get_ctd_predicate_identifier(self,label):
        chunk = label.split('|')
        breakups = [c.split('^') for c in chunk]
        renamed = [ f'{b[0]}^{self.term_parents[b[1]]}' for b in breakups]
        return f'CTD:{"|".join(renamed)}'

    def drug_to_gene(self, subject):
        output = []
        for identifier in subject.synonyms:
            if Text.get_curie(identifier).upper() == 'MESH':
                url=f"{self.url}/CTD_chem_gene_ixns_ChemicalID/{Text.un_curie(identifier)}/"
                obj = requests.get(url).json ()
                output = []
                for r in obj:
                    props = {"description": r[ 'Interaction' ]}
                    predicate_label = r['InteractionActions']
                    predicate_id = self.get_ctd_predicate_identifier(predicate_label)
                    standard_predicate_id, standard_predicate_label = self.standardize_predicate(predicate_id,predicate_label)
                    output.append( ( KEdge('ctd.drug_to_gene',dt.now(),predicate_id,predicate_label,identifier,
                                           standard_predicate_id, standard_predicate_label,[f"PMID:{r['PubMedIDs']}"],url,props),
                                     KNode(f"NCBIGENE:{r['GeneID']}", node_types.GENE) ) )
        return output

    def gene_to_drug(self, subject):
        output = []
        for identifier in subject.synonyms:
            if Text.get_curie(identifier).upper() == 'NCBIGENE':
                url = f"{self.url}/CTD_chem_gene_ixns_GeneID/{Text.un_curie(identifier)}/"
                obj = requests.get (url).json ()
                for r in obj:
                    props = {"description": r[ 'Interaction' ]}
                    predicate_label = r['InteractionActions']
                    predicate_id = self.get_ctd_predicate_identifier(predicate_label)
                    standard_predicate_id, standard_predicate_label = self.standardize_predicate(predicate_id,predicate_label)
                    output.append( ( KEdge('ctd.gene_to_drug',dt.now(),predicate_id,predicate_label,identifier,
                                           standard_predicate_id, standard_predicate_label,[f"PMID:{r['PubMedIDs']}"],url,props),
                                 KNode(f"MESH:{r['ChemicalID']}", node_types.DRUG) ) )
        return output

