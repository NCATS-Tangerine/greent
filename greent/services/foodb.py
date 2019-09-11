from greent.service import Service
from greent.graph_components import KNode
from greent.util import LoggingUtil
from greent.graph_components import LabeledID
from greent import node_types
from greent.graph_components import KEdge

import logging
import requests
import traceback
import os
import csv
import time

# declare a logger and initialize it
logger: logging.Logger = LoggingUtil.init_logging("robokop-interfaces.greent.services.FooDB", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

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
    #  3.) create and return a chemical substance node and edge
    #
    # param: food_node: KNode - a foodb node
    # return: array of chemical substance nodes/edges
    #############
    def food_to_chemical_substance(self, food_node: KNode) -> object:
        logger.debug("Starting food_to_chemical_substance operation")

        try:
            # init the return
            rv = []

            # get the contents records using the food id
            contents: list = requests.get(f"{self.url}contents_food_id/{food_node.id}").json()

            # loop through the contents returned
            for content in contents:
                # inspect and retreive info of the contents record
                good_row, content_type = self.check_content_row(content)

                # shall we continue
                if not good_row:
                    continue

                # re init the good_row flag
                good_row = False

                # init the edge properties variable
                props = []

                # what type of chemical substance are we working
                if content_type == 'Compound':
                    # use the source id in the contents record to get the compounds record
                    compound: list = requests.get(f"{self.url}compounds_id/{content['source_id']}").json()

                    # inspect the compound row
                    good_row, predicate_label, props = self.check_compound_row(compound)
                elif content_type == 'Nutrient':
                    # use the source id in the contents record to get the nutrient record
                    nutrient: list = requests.get(f"{self.url}nutrients_id/{content['source_id']}").json()

                    # inspect the compound row
                    good_row, predicate_label, props = self.check_nutrient_row(nutrient)

                # shall we continue
                if not good_row:
                    continue

                # create the edge label
                preds = LabeledID(identifier=f'FOODB:{predicate_label}', label=predicate_label)

                # create the new chemical substance node
                chemical_substance_node = KNode(f"FOODB:{predicate_label}", name=food_node.name, type=node_types.CHEMICAL_SUBSTANCE)

                # create the edge
                edge = self.create_edge(source_node=food_node,
                                        target_node=chemical_substance_node,
                                        provided_by='food_to_chemical_substance',
                                        input_id='identifier',
                                        predicate=preds,
                                        properties=props)

                # append the edge/node pair to the returned data array
                rv.append((chemical_substance_node, edge))
        except Exception as e:
            logger.error(f'Exception caught. Exception: {e}')
            return e

        # return to the caller
        return rv

    #############
    # check_content_row() - inspects a contents record and returns pertinent info
    #
    # param: content: list - a contents record
    # return: good_row:bool, predicate_label:str, content_type:str
    #############
    @staticmethod
    def check_content_row(content: list) -> (bool, str, str):
        # init the return
        good_row = False
        content_type = content["source_type"]

        # insure we have a good record
        if content_type == 'Compound' or content_type == "Nutrient":
            good_row = True

        # return to the caller
        return good_row, content_type

    #############
    # check_compound_row() - inspects a compounds record and returns pertinent info
    #
    # param: compound: list - the compounds records
    # return: good_row:bool, properties:list
    #############
    @staticmethod
    def check_compound_row(compound: list) -> (bool, list):
        # init the return
        good_row = True

        properties = {'content_type': 'Compound'}

        predicate_label = compound['public_id']

        #{'ENSEMBL': properties[0], 'p-value': float(properties[1]), 'slope': float(properties[2]), 'namespace': properties[3]}

        return good_row, predicate_label, properties

    #############
    # check_nutrient_row() - inspects a nutrient record and returns pertinent info
    #
    # param: nutrient: list - the nutrients records
    # return: good_row:bool, properties:list
    #############
    @staticmethod
    def check_nutrient_row(nutrient: list) -> (bool, list):
        # init the return
        good_row = True

        predicate_label = nutrient['public_id']

        properties = {'content_type': 'Nutrient'}

        return good_row, predicate_label, properties

    #############
    # load_all_foods() - gets a list of FooDB food records
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
                lines = csv.reader(inFH)

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
