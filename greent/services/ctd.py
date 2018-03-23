import logging
import requests
from greent.service import Service
import greent.util
from greent.graph_components import KNode, KEdge
from greent.util import Text
from greent import node_types

logger = greent.util.LoggingUtil.init_logging(__file__, level=logging.DEBUG)

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

    def drug_to_gene(self, subject):
        output = []
        for identifier in subject.synonyms:
            if Text.get_curie(identifier).upper() == 'MESH':
                obj = requests.get (f"{self.url}/CTD_chem_gene_ixns_ChemicalID/{Text.un_curie(identifier)}/").json ()
                output += [ ( self.get_edge(props=r, pmids=r['PubMedIDs']),
                              KNode(f"NCBIGENE:{r['GeneID']}", node_types.GENE) ) for r in obj ]
        return output

    def gene_to_drug(self, subject):
        output = []
        for identifier in subject.synonyms:
            if Text.get_curie(identifier).upper() == 'NCBIGENE':
                obj = requests.get (f"{self.url}/CTD_chem_gene_ixns_GeneID/{Text.un_curie(identifier)}/").json ()
                output += [( self.get_edge(props=r, pmids=r['PubMedIDs']),
                             KNode(f"MESH:{r['ChemicalID']}", node_types.DRUG) ) for r in obj ]
        return output

