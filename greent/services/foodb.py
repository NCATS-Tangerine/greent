from greent.util import LoggingUtil
from greent.service import Service
from greent import node_types
from greent.graph_components import KNode, LabeledID, KEdge

from csv import reader
import logging
import requests
import traceback
import os

# declare a logger and initialize it
logger = LoggingUtil.init_logging("robokop-interfaces.greent.services.FooDB", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

#############
# Class: FooDB
# By: Phil Owen
# Date: 8/26/2019
# Desc: A class that implements a robokop crawler service that interfaces with the FooDB data.
#############


class FooDB(Service):
    #############
    # constructor
    # param context: Service - the context of this service
    #############
    def __init__(self, context) -> None:
        # load by super class
        super(FooDB, self).__init__("foodb", context)

    #############
    # food_to_chemical_substance() - food to chemical data crawling interface operation.
    # multi-step operation:
    #  1.) use the id in the foodb node input param to look up a contents record (1 to 1)
    #  2.) use the contents' record source_id element to look up the compounds in the food (1 to many)
    #  3.) create and return a chemical substance node and edge list
    #
    # param: food_node: KNode - a foodb node
    # return: array of chemical substance nodes/edges
    #############
    def food_to_chemical_substance(self, in_food_node: KNode) -> list:
        logger.debug("Starting food_to_chemical_substance operation")

        # init the return
        rv: list = []

        try:
            # get the contents records using the food id
            contents: list = requests.get(f"{self.url}contents_food_id/{in_food_node.id}").json()

            # loop through the contents returned
            for content in contents:
                # inspect and retrieve info of the contents record
                good_row, content_type, food_common_name, edge_properties = self.check_content_row(content)

                # shall we continue
                if not good_row:
                    continue

                # what type of chemical substance are we working
                if content_type == 'Compound':
                    # use the source id in the contents record to get the compounds record
                    compound: dict = requests.get(f"{self.url}compounds_id/{content['source_id']}").json()[0]

                    # inspect the compound row and return the needed data
                    good_row, food_id, node_properties = self.check_compound_row(compound)
                elif content_type == 'Nutrient':
                    # use the source id in the contents record to get the nutrient record
                    nutrient: dict = requests.get(f"{self.url}nutrients_id/{content['source_id']}").json()[0]

                    # inspect the compound row
                    good_row, food_id, node_properties = self.check_nutrient_row(nutrient)
                else:
                    continue

                # shall we continue
                if not good_row:
                    continue
                
                # add more food meta data to the food node
                in_food_node.name = food_common_name
                in_food_node.properties = node_properties

                # create a new chemical substance node
                chemical_substance_node: KNode = KNode(id=f'{food_id}', name=food_common_name, type=node_types.CHEMICAL_SUBSTANCE, properties=node_properties)

                # create the edge label
                predicate: LabeledID = LabeledID(identifier='RO:0001019', label='contains')

                # create the edge
                edge: KEdge = self.create_edge(source_node=in_food_node,
                                               target_node=chemical_substance_node,
                                               provided_by='foodb.food_to_chemical_substance',
                                               input_id=in_food_node.id,
                                               predicate=predicate,
                                               properties=edge_properties
                                               )

                # append the edge/node pair to the returned data array
                rv.append((edge, chemical_substance_node))
        except Exception as e:
            logger.error(f'Exception caught. Exception: {e}')

        # return to the caller
        return rv

    #############
    # check_content_row - inspects a contents record and returns pertinent info
    #
    # param: content: dict - a contents record
    # return: good_row:bool, content_type:str, food_common_name: str, edge_properties: dict
    #############
    @staticmethod
    def check_content_row(content: dict) -> (bool, str, str, dict):
        # init the return values
        good_row: bool = False
        content_type: str =''
        food_common_name: str = ''
        edge_properties:dict = {}

        # insure we have a good record
        if content['source_type'] == 'Compound' or content['source_type'] == "Nutrient":
            good_row = True

            # get the content type name
            content_type: str = content['source_type']

            # get the food name
            food_common_name: str = content['orig_food_common_name']

            # get the edge properties
            edge_properties: dict = {'source_type':  content['source_type'], 'source_name': content['orig_source_name'], 'unit': content['orig_unit'], 'content': content['orig_content']}

        # return to the caller
        return good_row, content_type, food_common_name, edge_properties

    #############
    # check_compound_row - inspects a compounds record and returns pertinent info
    #
    # param: compound: dict - the compounds records
    # return: good_row: bool, food_id: str, node_properties: dict
    #############
    @staticmethod
    def check_compound_row(compound: dict) -> (bool, str, dict):
        # init the return values
        good_row: bool = False
        food_id: str = ''
        node_properties: dict = {}

        # init the equivalent identifier
        equivalent_identifier: str = ''

        # get the identifier.
        if compound['moldb_inchikey'] != '':
            equivalent_identifier = f'INCHIKEY:{compound["moldb_inchikey"][9:]}'
        elif compound['chembl_id'] != '':
            equivalent_identifier = f'CHEMBL:{compound["chembl_id"]}'
        elif compound['drugbank_id'] != '':
            equivalent_identifier = f'DRUGBANK:{compound["drugbank_id"]}'
        elif compound['kegg_compound_id'] != '':
            equivalent_identifier = f'KEGG.COMPOUND:{compound["kegg_compound_id"]}'
        elif compound['chebi_id'] != '':
            equivalent_identifier = f'CHEBI:{compound["chebi_id"]}'
        elif compound['hmdb_id'] != '':
            equivalent_identifier = f'HMDB:{compound["hmdb_id"]}'
        elif compound['pubchem_compound_id'] != '':
            equivalent_identifier = f'PUBCHEM:{compound["pubchem_compound_id"]}'

        # if no identifier found the record is no good
        if equivalent_identifier != '':
            # set the good row flag
            good_row = True

            # set the edge id
            food_id = equivalent_identifier

            # set the node properties
            node_properties = {'content_type': 'Compound', 'foodb_id': compound['public_id'], 'equivalent_identifiers': [equivalent_identifier]}

        # return to the caller
        return good_row, food_id, node_properties

    #############
    # check_nutrient_row - inspects a nutrient record and returns pertinent info
    #
    # param: nutrient: dict - the nutrients records
    # return: good_row: bool, food_id: str, node_properties: dict
    #############
    @staticmethod
    def check_nutrient_row(nutrient: dict) -> (bool, str, dict):
        # init the return values
        good_row: bool = False

        # set the edge id
        food_id: str = nutrient['public_id']

        # set the node properties
        node_properties: dict = {'content_type': 'Nutrient', 'nutrient': True}

        # return to the caller
        return good_row, food_id, node_properties
    #############
    # load_all_foods - gets a list of FooDB food records
    #
    # param path_to_file: str - the path to the foods CSV file
    # return: foods:list - the list of foods
    #############
    @staticmethod
    def load_all_foods(path_to_file: str) -> list:
        logger.info(f'Processing input file: {path_to_file}')

        # init the return value
        food_list = []

        try:
            # get the input file handle, skip the header line and parse the rest
            with open(path_to_file, 'r', encoding='latin_1') as inFH:
                # read in the lines
                lines = reader(inFH)

                # read the header
                header_line = next(lines)

                # index into the array to the HGVS position
                id_index = header_line.index('id')

                # for each line (skipping the first header line)
                for line in lines:
                    # add the food db id to the list
                    food_list.append(line[id_index])

            # close the input file
            inFH.close()
        except Exception as e:
            logger.error(traceback.print_exc(e))

        logger.info(f'File processing complete: {path_to_file}')

        # return to the caller
        return food_list
