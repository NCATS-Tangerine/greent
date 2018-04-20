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
        return p_id, p_label

    def get_ctd_predicate_identifier(self,label):
        return 'CTD:00001'

    def drug_to_gene(self, subject):
        output = []
        for identifier in subject.synonyms:
            if Text.get_curie(identifier).upper() == 'MESH':
                url=f"{self.url}/CTD_chem_gene_ixns_ChemicalID/{Text.un_curie(identifier)}/"
                obj = requests.get(url).json ()
                output = []
                for r in obj:
                    props = {"description": r[ 'Interaction' ]}
                    actions = r['InteractionActions'].split('|')
                    for predicate_label in actions:
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
                    actions = r['InteractionActions'].split('|')
                    for predicate_label in actions:
                            predicate_id = self.get_ctd_predicate_identifier(predicate_label)
                            standard_predicate_id, standard_predicate_label = self.standardize_predicate(predicate_id,predicate_label)
                            output.append( ( KEdge('ctd.gene_to_drug',dt.now(),predicate_id,predicate_label,identifier,
                                                   standard_predicate_id, standard_predicate_label,[f"PMID:{r['PubMedIDs']}"],url,props),
                                         KNode(f"MESH:{r['ChemicalID']}", node_types.DRUG) ) )
        return output

