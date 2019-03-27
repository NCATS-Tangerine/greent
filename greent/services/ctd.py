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
                good_row, predicate_label, props = self.check_gene_chemical_row(r)
                if not good_row:
                    continue
                predicate = LabeledID(identifier=f'CTD:{predicate_label}', label=predicate_label)
                gene_node = KNode(f"NCBIGENE:{r['GeneID']}", type=node_types.GENE)
                if sum([s in predicate.identifier for s in self.g2d_strings]) > 0:
                    subject = gene_node
                    object = drug
                else:
                    subject = drug
                    object = gene_node
                edge = self.create_edge(subject,object,'ctd.drug_to_gene',identifier,predicate,
                                        publications=[f"PMID:{x}" for x in r['PubMedIDs'].split('|')],url=url,properties=props)
                output.append( (edge,gene_node) )
        return output

    def check_gene_chemical_row(self, r):
        props = {"description": r['Interaction'], 'taxon': f"taxon:{r['OrganismID']}"}
        pmid_count = len(r['PubMedIDs'].split('|'))
        predicate_label = r['InteractionActions']
        # there are lots of garbage microarrays with only one paper. THey goop the place up
        # ignore them
        good_row = True
        if pmid_count < 3:
            if predicate_label in ['affects^expression', 'increases^expression',
                                   'decreases^expression', 'affects^methylation',
                                   'increases^methylation', 'decreases^methylation']:
                good_row = False
        if pmid_count < 2:
            if predicate_label in ['affects^splicing', 'increases^splicing', 'decreases^splicing']:
                good_row = False
        if '|' in predicate_label:
            good_row = False
        return good_row, predicate_label, props

    def check_expanded_gene_chemical_row(self, r):
        props = {"description": r['interaction'], 'taxon': f"taxon:{r['taxonID']}"}
        pmids = r['PMID'].split('|')
        predicate_label = r['interaction']
        # there are lots of garbage microarrays with only one paper. THey goop the place up
        # ignore them
        good_row = True
        if len(pmids) < 3:
            if predicate_label in ['affects expression of', 'increases expression of',
                                   'decreases expression of', 'affects methylation of',
                                   'increases methylation of', 'decreases methylation of',
                                   'affects molecular modification of',
                                   'increases molecular modification of',
                                   'decreases molecular modification of']:
                good_row = False
        if len(pmids) < 2:
            if predicate_label in ['affects splicing of', 'increases splicing of', 'decreases splicing of']:
                good_row = False
        pmids = [Text.upper_curie(p) for p in pmids]
        return good_row, predicate_label, props, pmids

    def drug_to_gene_expanded(self, drug):
        output = []
        identifiers = drug.get_synonyms_by_prefix('MESH')
        for identifier in identifiers:
            url=f"{self.url}CTD_chem_gene_expanded_chemicalID/mesh:{Text.un_curie(identifier)}/"
            result = requests.get(url)
            obj=result.json()
            for r in obj:
                good_row, predicate_label, props, pmids = self.check_expanded_gene_chemical_row(r)
                if not good_row:
                    continue
                predicate = LabeledID(identifier=f"CTD:{Text.snakify(predicate_label)}", label=predicate_label)
                gene_node = KNode(Text.upper_curie(r['geneID']), name=r['gene_label'],type=node_types.GENE)
                direction = r['direction']
                if direction == '->':
                    subject = drug
                    object = gene_node
                else:
                    subject = gene_node
                    object = drug
                edge = self.create_edge(subject,object,'ctd.drug_to_gene_expanded',identifier,predicate,publications=pmids,properties=props,url=url )
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
                if r['GeneID'] != geneid:
                    continue
                good_row, predicate_label, props = self.check_gene_chemical_row(r)
                if not good_row:
                    continue
                predicate = LabeledID(identifier=f'CTD:{predicate_label}', label=predicate_label)
                #Should this be substance?
                drug_node = KNode(f"MESH:{r['ChemicalID']}", type=node_types.CHEMICAL_SUBSTANCE, name=f"{r['ChemicalName']}")
                if sum([s in predicate.identifier for s in self.g2d_strings]) > 0:
                    subject = gene_node
                    obj = drug_node
                else:
                    subject = drug_node
                    obj = gene_node
                edge = self.create_edge(subject,obj,'ctd.gene_to_drug',identifier,predicate,
                                        publications=[f"PMID:{x}" for x in r['PubMedIDs'].split('|') ],url=url,properties=props)
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
                good_row, predicate_label, props, pmids = self.check_expanded_gene_chemical_row(r)
                if not good_row:
                    continue
                predicate = LabeledID(identifier=f"CTD:{Text.snakify(predicate_label)}", label=predicate_label)
                #Should this be substance?
                drug_node = KNode(Text.upper_curie(r['chemicalID']), type=node_types.CHEMICAL_SUBSTANCE, name=r['chem_label'])
                direction = r['direction']
                if direction == '->':
                    subject = drug_node
                    object = gene_node
                else:
                    subject = gene_node
                    object = drug_node
                edge = self.create_edge(subject,object,'ctd.gene_to_drug_expanded',identifier,predicate,properties = props,url=url,publications=pmids)
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
            logger.info(url)
            obj = requests.get (url).json ()
            logger.info(len(obj))
            chemical_evidence_basket = {}
            for r in obj:
                #collect those that have evidence
                if r['DirectEvidence'] != '':
                    c_id = r['ChemicalID']
                    if c_id not in chemical_evidence_basket:
                        chemical_evidence_basket[c_id] = {
                            'name': r['ChemicalName'],
                            'evidences': []
                        }
                    evidence = {
                        'DirectEvidence': r['DirectEvidence'],
                        'refs': [f'PMID:{pmid}' for pmid in r['PubMedIDs'].split('|')]
                    }
                    chemical_evidence_basket[c_id]['evidences'].append(evidence)
            # now start making the edges and nodes based
            for c_id in chemical_evidence_basket:
                chemical_info = chemical_evidence_basket[c_id]
                treats_count = 0
                marker_count = 0
                treats_refs = []
                marker_refs = []
                for evidence in chemical_info['evidences']:
                    if evidence['DirectEvidence'] == 'therapeutic':
                        treats_count += 1
                        treats_refs += evidence['refs'] 
                    elif evidence['DirectEvidence'] == 'marker/mechanism':
                        marker_count += 1
                        marker_refs += evidence['refs']
                predicate = self.get_chemical_label_id(treats_count, marker_count)
                if predicate == None:
                    continue
                publications = []
                # do some reorgnanizing of the pubmedids
                if predicate.identifier == 'RO:0001001':
                    publications  = treats_refs + marker_refs
                if 'marker' in predicate.identifier :
                    publications = marker_refs
                if 'therapeutic' in predicate.identifier:
                    publications = treats_refs
                # make node and edge
                drug_node = KNode(f'MESH:{c_id}', type=node_types.CHEMICAL_SUBSTANCE, name= chemical_info['name'])
                edge = self.create_edge(
                    drug_node,
                    disease_node,
                    'ctd.disease_to_chemical',
                    identifier,
                    predicate = predicate,
                    publications= publications,
                    url= url
                )
                key = (drug_node.id, edge.standard_predicate)
                if key not in unique:
                    output.append( (edge,drug_node) )
                    unique.add(key)   
        return output

    def get_chemical_label_id (self, therapeutic_count, marker_count, marker_predicate_label = 'marker/mechanism', therapeutic_predicate_label = 'therapeutic'):
        """
        This function applies rules to determine which edge to prefer in cases
        where conflicting edges are returned for a chemical disease relation ship. 
        """
        if therapeutic_count == marker_count and therapeutic_count < 3:
            return None
        # avoid further checks if we find homogeneous types     
        if marker_count == 0 and therapeutic_count > 0 :
            return LabeledID(identifier = f'CTD:{therapeutic_predicate_label}', label = therapeutic_predicate_label)
        if therapeutic_count == 0 and marker_count > 0:
            return LabeledID(identifier= f'CTD:{marker_predicate_label}', label = marker_predicate_label)
        
        
        marker = (therapeutic_count == 1 and marker_count > 1)\
                or(marker_count / therapeutic_count > 2)

        therapeutic = (marker_count == 1 and therapeutic_count > 1)\
                    or(therapeutic_count / marker_count > 2)
        if marker:
            return LabeledID(identifier= f'CTD:{marker_predicate_label}', label = marker_predicate_label)
        if therapeutic:
            return LabeledID(identifier = f'CTD:{therapeutic_predicate_label}', label = therapeutic_predicate_label)
        return LabeledID(identifier= 'RO:0001001', label='related to')