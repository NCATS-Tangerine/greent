import logging
import requests
from datetime import datetime as dt
from greent.service import Service
from greent.graph_components import KNode, LabeledID
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
        return [ KNode(identifier, type=node_types.CHEMICAL_SUBSTANCE) for identifier in identifiers ]

    def standardize_predicate(self, predicate, sourcenode=None, targetnode=None):
        """CTD has a little more work to do than the standard service."""
        if '|' not in predicate.label:
            return self.concept_model.standardize_relationship(predicate)
        parts = predicate.label.split('|')
        goodparts = list(filter(lambda p:'reaction' not in p and 'cotreatment' not in p, parts))
        if len(goodparts) != 1:
            return self.concept_model.standardize_relationship(LabeledID(identifier='CTD:interacts_with', label='interacts_with'))
        #Change the modifier to "affects" to deal with the fact that we don't know what the deleted part does.
        thing = self.term_parents[goodparts[0].split('^')[1]]
        new_id = f'CTD:affects^{thing}'
        return self.concept_model.standardize_relationship(LabeledID(identifier=new_id, label=new_id))

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
                #Let's only keep humans for now:
                if r['OrganismID'] != '9606':
                    continue
                props = {"description": r[ 'Interaction' ]}
                predicate_label = r['InteractionActions']
                predicate = LabeledID(identifier=self.get_ctd_predicate_identifier(predicate_label), label=predicate_label)
                gene_node = KNode(f"NCBIGENE:{r['GeneID']}", type=node_types.GENE)
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

    def drug_to_gene_expanded(self, drug):
        output = []
        identifiers = drug.get_synonyms_by_prefix('MESH')
        for identifier in identifiers:
            url=f"{self.url}CTD_chem_gene_expanded_chemicalID/mesh:{Text.un_curie(identifier)}/"
            print(url)
            result = requests.get(url)
            print(result.status_code)
            obj=result.json()
            for r in obj:
                #Let's only keep humans for now:
                if r['taxonID'] != 'ncbitaxon:9606':
                    continue
                predicate_label = r['degree']+' '+r['interaction']
                predicate = LabeledID(identifier=f'CTD:{Text.snakify(predicate_label)}', label=predicate_label)
                gene_node = KNode(Text.upper_curie(r['geneID']), name=r['gene_label'],type=node_types.GENE)
                direction = r['direction']
                if direction == '->':
                    subject = drug
                    object = gene_node
                else:
                    subject = gene_node
                    object = drug
                edge = self.create_edge(subject,object,'ctd.drug_to_gene_extended',identifier,predicate )
                output.append( (edge,gene_node) )
        return output

    def gene_to_drug(self, gene_node):
        output = []
        identifiers = gene_node.get_synonyms_by_prefix('NCBIGENE')
        for identifier in identifiers:
            unique = set()
            geneid = Text.un_curie(identifier)
            url = f"{self.url}/CTD_chem_gene_ixns_GeneID/{geneid}/"
            obj = requests.get (url).json ()
            for r in obj:
                #Let's only keep humans for now:
                if r['OrganismID'] != '9606':
                    continue
                if r['GeneID'] != geneid:
                    continue
                props = {"description": r[ 'Interaction' ]}
                predicate_label = r['InteractionActions']
                predicate = LabeledID(identifier=self.get_ctd_predicate_identifier(predicate_label), label=predicate_label)
                #Should this be substance?
                drug_node = KNode(f"MESH:{r['ChemicalID']}", type=node_types.CHEMICAL_SUBSTANCE, name=f"{r['ChemicalName']}")
                if sum([s in predicate.identifier for s in self.g2d_strings]) > 0:
                    subject = gene_node
                    obj = drug_node
                else:
                    subject = drug_node
                    obj = gene_node
                edge = self.create_edge(subject,obj,'ctd.gene_to_drug',identifier,predicate,
                                        publications=[f"PMID:{r['PubMedIDs']}"],url=url,properties=props)
                #This is what we'd like it to be, but right now there's not enough real specificity on the predicates
                #key = (drug_node.id, edge.standard_predicate.label)
                key = (drug_node.id, edge.original_predicate.label)
                if key not in unique:
                    output.append( (edge,drug_node) )
                    unique.add(key)
        return output


    def gene_to_drug_expanded(self, gene_node):
        output = []
        identifiers = gene_node.get_synonyms_by_prefix('NCBIGENE')
        for identifier in identifiers:
            unique = set()
            geneid = Text.un_curie(identifier)
            url = f"{self.url}/CTD_chem_gene_expanded_geneID/ncbigene:{geneid}/"
            obj = requests.get (url).json ()
            for r in obj:
                #Let's only keep humans for now:
                if r['taxonID'] != 'ncbitaxon:9606':
                    continue
                predicate_label = r['degree']+' '+r['interaction']
                predicate = LabeledID(identifier=f'CTD:{Text.snakify(predicate_label)}', label=predicate_label)
                #Should this be substance?
                drug_node = KNode(Text.upper_curie(r['chemicalID']), type=node_types.CHEMICAL_SUBSTANCE, name=r['chem_label'])
                direction = r['direction']
                if direction == '->':
                    subject = drug_node
                    object = gene_node
                else:
                    subject = gene_node
                    object = drug_node
                edge = self.create_edge(subject,object,'ctd.gene_to_drug_extended',identifier,predicate )
                #This is what we'd like it to be, but right now there's not enough real specificity on the predicates
                #key = (drug_node.id, edge.standard_predicate.label)
                key = (drug_node.id, edge.original_predicate.label)
                if key not in unique:
                    output.append( (edge,drug_node) )
                    unique.add(key)
        return output

    def disease_to_exposure(self, disease_node):
        logger.info("disease-to-exposure")
        output = []
        identifiers = disease_node.get_synonyms_by_prefix('MESH')
        for identifier in identifiers:
            unique = set()
            url = f"{self.url}CTD_exposure_events_diseaseid/{Text.un_curie(identifier)}/"
            obj = requests.get (url).json ()
            logger.info(url)
            logger.info(len(obj))
            for r in obj:
                predicate_label = r['outcomerelationship']
                if predicate_label == 'no correlation':
                    continue
                predicate = LabeledID(identifier=f"CTD:{''.join(predicate_label.split())}", label=predicate_label)
                #Should this be substance?
                drug_node = KNode(f"MESH:{r['exposurestressorid']}", type=node_types.CHEMICAL_SUBSTANCE, name=r['exposurestressorname'])
                edge = self.create_edge(drug_node,disease_node,'ctd.disease_to_exposure',identifier,predicate,
                                        publications=[f"PMID:{r['reference']}"],url=url)
                key = (drug_node.id, edge.standard_predicate)
                if key not in unique:
                    output.append( (edge,drug_node) )
                    unique.add(key)
        return output

    def disease_to_chemical(self, disease_node):
        logger.info("disease_to_chemical")
        output = []
        identifiers = disease_node.get_synonyms_by_prefix('MESH')
        for identifier in identifiers:
            unique = set()
            url = f"{self.url}CTD_chemicals_diseases_DiseaseID/{identifier}/"
            obj = requests.get (url).json ()
            logger.info(url)
            logger.info(len(obj))
            for r in obj:
                predicate_label = r['DirectEvidence']
                if predicate_label == '':
                    continue
                    predicate_label = 'inferred'
                predicate = LabeledID(identifier=f'CTD:{predicate_label}', label=predicate_label)
                refs = [f'PMID:{pmid}' for pmid in r['PubMedIDs'].split('|')]
                #Should this be substance?
                drug_node = KNode(f"MESH:{r['ChemicalID']}", type=node_types.CHEMICAL_SUBSTANCE, name=r['ChemicalName'])
                edge = self.create_edge(drug_node,disease_node,'ctd.disease_to_chemical',identifier,predicate,
                                        publications=refs,url=url)
                key = (drug_node.id, edge.standard_predicate)
                if key not in unique:
                    output.append( (edge,drug_node) )
                    unique.add(key)
        return output

