from greent.service import Service
from ftplib import FTP
import csv
from functools import partial 
from io import StringIO
import logging
from greent.util import LoggingUtil, Text
from greent import node_types
from greent.graph_components import KEdge, KNode, LabeledID
import requests 


logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class Panther(Service):
    
    def __init__(self, context):
        super(Panther, self).__init__("panther", context)
        self.sequence_file_columns = {
            0: 'gene_identifier',            # format:  organism|gene id source:gene id|protein id source:protein id
            1: 'protein_id',                 # currently empty. The protein ids can be retrieved from above
            2: 'panther_sf_id',              # for example, PTHR12213:SF6.  ":SF" indicates the subfamily ID
            3: 'panther_family_name',
            4: 'panther_subfamily_name',
            5: 'panther_molecular_func',
            6: 'panther_biological_process',
            7: 'cellular_components',        # PANTHER GO slim cellular component terms assigned to families and subfamilies
            8: 'protein_class',              # PANTHER protein class terms assigned to families and subfamilies
            9: 'pathway'                    # The format of the pathway information is: pathway_long_name#pathway_short_name#pathway_accession>component_long_name#component_short_name#component_accession
        }
        # to seperate each column into sub entries for better access
        self.splitter_mapping = {
            'gene_identifier': partial(self.split_with,splitter = '|', keys=['organism','gene_id','protein_id']),
            'panther_molecular_func' : partial(self.split_with, splitter= ';'),
            'panther_biological_process' : partial(self.split_with, splitter= ';'),
            'cellular_components' : partial(self.split_with, splitter= ';'),
            'pathway': partial(self.split_with, splitter= ';') 
            }
        self.__gene_family_data__ = None

    def pull_ftp_data(self, ftp_file, ftp_dir, ftp_url):
        """
        Pulls ftp data and returns string stream. Stream should be 
        closed by the method calling this function.
        """
        ftp = FTP(ftp_url)        
        ftp.maxline= 32000
        ftp.login()
        ftp.cwd(ftp_dir)
        data = StringIO()
        writer = lambda x : data.write(x + '\n')
        ftp.retrlines(f'RETR {ftp_file}', writer)
        ftp.quit()
        data.seek(0)
        return data

    def get_gene_family_data(self):
        """
        Makes ftp request to get sequence classification file from Panther ftp.
        """
        # get the human file
        sequence_classication_path = '/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/'
        sequence_classication_file = 'PTHR14.1_human_'
        ftp_data = self.pull_ftp_data(sequence_classication_file, sequence_classication_path, self.url) 
        return ftp_data

    @property
    def gene_family_data(self):
        """
        Property that restructures raw csv values into dictionary organized by family and subfamilies of genes.
        """
        rows = []
        if self.__gene_family_data__ :
            return self.__gene_family_data__
        with self.get_gene_family_data() as ftp_data:
            reader = csv.reader(iter(ftp_data.readline,''), delimiter= '\t')
            # first pass transform csv to dictionary
            for row in reader:
                rows.append(row)
        logger.debug(f'Found {len(rows)} records')
        with_columns = [{self.sequence_file_columns[index]: value for index, value in enumerate(row)} for row in rows]
        # second pass transform into sub dictionaries for relevant ones
        for row in with_columns:
            for key in self.splitter_mapping:
                functor = self.splitter_mapping[key]
                row[key] = functor(row[key])
        # reorganize them to 'family-key'-'sub-family'-{everthing else} 
        self.__gene_family_data__ = {}
        for row in with_columns:
            fam_id, sub_id = row['panther_sf_id'].split(':')
            family_name = row['panther_family_name']
            sub_family_name = row['panther_subfamily_name']
            if fam_id not in self.__gene_family_data__:
                self.__gene_family_data__[fam_id] = {
                    'family_name' : family_name
                }
            if sub_id not in self.__gene_family_data__[fam_id]:
                self.__gene_family_data__[fam_id][sub_id] = {
                    'sub_family_name': sub_family_name,
                    'rows': []
                }
            self.__gene_family_data__[fam_id][sub_id]['rows'].append(row)
        return self.__gene_family_data__

    def split_with(self, input_str, splitter, keys=[], ignore_length_mismatch = False):
        """
        Splits a string based on splitter. If keys is provided it will return a dictionary where the keys of the dictionary map to 
        the splitted values. 
        """
        splitted = input_str.split(splitter)
        if keys == []:
            return splitted
        if not ignore_length_mismatch and len(splitted) != len(keys):
                raise Exception("Length of keys provided doesn't match splitted result")
        
        return {keys[index]: value for index, value in enumerate(splitted[:len(keys)])}

    
    def get_family_sub_family_ids_from_curie(self, curie):
        """
        Splits a panther curie into family id and sub family id
        when ever possible.
        """
        if 'PANTHER.FAMILY' in curie:
            curie = Text.un_curie(curie)
        splitted = curie.split(':')
        if len(splitted) == 1:
            return (splitted[0], None)
        return (splitted)

    def get_rows_using_curie(self, curie):
        """
        Get all information from the Panther.gene_family_data using a panther identifier.
        """
        fam_id, sub_fam_id = self.get_family_sub_family_ids_from_curie(curie)
        if sub_fam_id == None:
            rows = []
            sub_ids = [y for y in list(self.gene_family_data[fam_id].keys()) if y != 'family_name']
            for sub_id in sub_ids:
                rows += [ x for x in self.gene_family_data[fam_id][sub_id]['rows'] if x not in rows]
            return rows
        return self.gene_family_data[fam_id][sub_fam_id]['rows']
    

    def get_gene_by_gene_family(self, gene_family_node):
        """
        Creates Gene nodes associated with a gene family.
        """
        results = []
        predicate = LabeledID('BFO:0000050', 'part of')
        rows = self.get_rows_using_curie(gene_family_node.id)
        for gene_family_data in rows: 
            gene_data = gene_family_data['gene_identifier']
            gene_id = gene_data['gene_id'].replace('=',':')
            gene_name = requests.get(f'https://bionames.renci.org/ID_to_label/{gene_id}/').json()[0]['label'] 
            gene_name = gene_name if gene_name else gene_id        
            gene_node = KNode(gene_id, type= node_types.GENE, name= gene_name)
            
            edge = self.create_edge(
                gene_node,
                gene_family_node,
                'panther.get_gene_by_gene_family',
                gene_family_node.id,
                predicate
            )
            results.append((edge, gene_node))
        return results
        

    def get_biological_process_or_activity_by_gene_family(self, gene_family_node):
        """
        Creates Biological process/activity nodes associated with a gene family.
        """
        results = []  
        # @TODO make sensible edge here
        predicate = LabeledID('BFO:0000054','related_to')      
        rows = self.get_rows_using_curie(gene_family_node.id)
        for row in rows:
            bio_process_or_activity_data = [x for x in row['panther_molecular_func'] if x != ''] + [x for x in row['panther_biological_process'] if x != '']
            for bp in bio_process_or_activity_data:
                label, id = bp.split('#')
                bio_process_or_activity_node = KNode(id, type= node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, name= label)
                
                edge = self.create_edge(
                    gene_family_node,
                    bio_process_or_activity_node,
                    'panther.get_biological_process_or_activity_by_gene_family',
                    gene_family_node.id,
                    predicate
                )
                results.append((edge, bio_process_or_activity_node))
        return results

    def get_cellular_component_by_gene_family(self, gene_family_node):
        """
        Makes gene family to cellular component associations.
        """
        results = []
        # @TODO make a sensible relation here 
        predicate = LabeledID('BFO:0000054','related_to')
        rows = self.get_rows_using_curie(gene_family_node.id)
        for gene_family_data in rows: 
            cc_data = [x for x in gene_family_data['cellular_components'] if x != '']
            for row in cc_data:
                label, id = row.split('#')
                cc_node = KNode(id, type=node_types.CELLULAR_COMPONENT, name=label)
                edge = self.create_edge(
                    gene_family_node,
                    cc_node,
                    'panther.get_cellular_component_by_gene_family',
                    gene_family_node.id,
                    predicate
                )
                results.append((edge, cc_node))
        return results

    def get_pathway_by_gene_family(self, gene_family_node):
        """        
        """
        results = []
        predicate = LabeledID('BFO:0000054','related_to')
        rows = self.get_rows_using_curie(gene_family_node.id)
        for gene_family_data in rows:
            pathway_data = [x for x in gene_family_data['pathway'] if x != '']
        #parse out the data
            for row in pathway_data:
                pathway_data_raw, component_data_raw = self.split_with(row,splitter = '>')  
                pathway_data_split = self.split_with(pathway_data_raw, splitter= '#', keys= ['pathway_name', 'pathway_access'])
                # component_data = self.split_with(component_data_raw, splitter= '#', ['component_name', 'component_access'])
                pathway_node = KNode(f"PANTHER.PATHWAY:{pathway_data_split['pathway_access']}", type= node_types.PATHWAY, name= pathway_data_split['pathway_name'])
                
                edge = self. create_edge(
                    gene_family_node,
                    pathway_node,
                    'panther.get_pathway_by_gene_family',
                    gene_family_node.id,
                    predicate
                )
                results.append((edge, pathway_node))
        return results 

    def get_gene_family_by_gene_family(self, gene_family_node):
        """
        Create Gene family nodes given a gene family.
        """
        response = []
        fam_id, sub_fam_id = self.get_family_sub_family_ids_from_curie(gene_family_node.id)

        predicate = LabeledID('BFO:0000050', 'part of')
        if sub_fam_id == None:
            # we are looking for subfamilies
            sub_id_keys = [y for y in self.gene_family_data[fam_id] if y != 'family_name']
            for sub_id in sub_id_keys:
                panther_id = f'{fam_id}:{sub_id}'
                # logger.debug(f'GENE _ FAMILY DATA: { self.gene_family_data[fam_id]}')
                sub_family_node = self.__create_gene_family_node(panther_id, self.gene_family_data[fam_id][sub_id]['sub_family_name'])
                edge = self.create_edge(
                    sub_family_node,
                    gene_family_node,
                    'panther.get_gene_family_by_gene_family',
                    sub_family_node.id,
                    predicate
                )
                response.append((edge, sub_family_node))
            return response
        # else we are a sub family 
        family_node = self.__create_gene_family_node(fam_id, self.gene_family_data[fam_id]['family_name'])
        edge = self.create_edge(
            gene_family_node,
            family_node,
            'panther.get_gene_family_by_gene_family',
            gene_family_node.id,
            predicate
        )
        return [(edge, family_node)]        
        

    def __create_gene_family_node(self,  panther_id, name):
        """
        Private method that prefixs the identifier and creates a gene family node
        """
        curie = f'PANTHER.FAMILY:{panther_id}'
        if  'NOT NAMED' in name:
            name = f'{name} ({panther_id})'
        return KNode(curie, type = node_types.GENE_FAMILY, name = name)


    
                    