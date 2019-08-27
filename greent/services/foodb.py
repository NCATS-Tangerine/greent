from logging import Logger

from greent.service import Service
from greent.graph_components import KNode
from greent.util import LoggingUtil
from greent.graph_components import LabeledID
from greent import node_types

import requests
import traceback
import os
import csv

# declare a logger and initialize it
import logging
logger: Logger = LoggingUtil.init_logging("robokop-interfaces.greent.services.FooDB", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

#############
# Class: FooDB
# By: Phil Owen
# Date: 8/26/2019
# Desc: A class that implements a robokop serve that interfaces with the FooDB data
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
    # food to chemical data crawling interface operation.
    # this will require a step operation:
    #  1.) use the id in the food node to look up the contents record
    #  2.) use the contents record.source_id to look up the compound
    #  3.) lookup the compound record
    #  4.) create a chemical substance node and edge
    # param food_node: KNode - a chemical substance node
    #############
    def food_to_chemical_substance(self, food_node: KNode) -> object:
        logger.debug("Starting chemical_substance_to_food operation")

        try:
            # init the return
            rv = []

            # get the contents records using the food id
            contents: list = requests.get(f"{self.url}contents_food_id/{food_node.id}").json()

            # loop through the contents returned
            for content in contents:
                # inspect the content row
                good_row, props = self.check_content_row(content)

                # shall we continue
                if not good_row:
                    continue

                # use the source id in the content reord to ge the chemical substance record
                compound: list = requests.get(f"{self.url}compounds_id/{content['source_id']}").json()

                # inspect the compound row
                good_row, predicate_label, props = self.check_compound_row(compound)

                # shall we continue
                if not good_row:
                    continue

                predicate = LabeledID(identifier=f'FOODB:{predicate_label}', label=predicate_label)
                chemical_substance_node = KNode(f"", type=node_types.CHEMICAL_SUBSTANCE)

                edge = self.create_edge('subject', object, 'foodb.food_to_compound', 'identifier', predicate, properties=props)

                rv.append((edge, chemical_substance_node))
        except Exception as e:
            logger.error(f'Exception caught. Exception: {e}')
            return e

        # return to the caller
        return rv

    #############
    # inspects a contents record and returns pertinent info
    # param path_to_file: str - the contents record
    #############
    @staticmethod
    def check_content_row(content: list):
        return True, None

    #############
    # inspects a compounds record and returns pertinent info
    # param compound: list - the compounds records
    #############
    @staticmethod
    def check_compound_row(compound: list):
        return True, None, None

    #############
    # gets a list of FooDB food records
    # param path_to_file: str - the path to the foods CSV file
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
