from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode
from greent.export_delegator import WriterDelegator
from greent.util import LoggingUtil
from greent.util import Text
from builder.gtex_utils import GTExUtils
from builder.question import LabeledID
import csv
import os

# declare a logger and initialize it.
import logging
logger = LoggingUtil.init_logging("robokop-interfaces.builder.GTExBuilder", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')


##############
# Class: GTExBuilder
#
# By: Phil Owen
# Date: 5/21/2019
# Desc: Class that pre-loads significant GTEx data elements into the redis cache and neo4j graph database.
##############
class GTExBuilder:
    #######
    # Constructor
    # param rosetta : Rosetta - project object for shared objects
    #######
    def __init__(self, rosetta: Rosetta):
        self.rosetta = rosetta

        # create static edge labels for variant/gtex and gene/gtex edges
        self.variant_gtex_label = LabeledID(identifier=f'GTEx:affects_expression_in', label=f'affects expression in')
        self.gene_gtex_label = LabeledID(identifier=f'gene_to_expression_site_association', label=f'gene to expression site association')

        # get a ref to the util class
        self.gtu = GTExUtils(self.rosetta)

    #####################
    # load - Processes the gtex data and loads the redis cache and graph databases with it
    #
    # param data_directory: str - the name of the directory that GTEx files will be processed in
    # param out_file_name: str - the name of the target file
    # param process_raw_data: bool - flag to gather and process raw GTEx data files
    # param process_for_cache: bool - flag to process the GTEx file and load the redis cache with it
    # param process_for_graph: bool - flag to process the GTEx file and load the neo4j graph with it
    # returns: object, pass if it is None, otherwise an exception object to indicate what failed
    #####################
    def load(self, data_directory: str, out_file_name: str = 'sqtl_signifpairs.csv', process_raw_data: bool = True, process_for_cache: bool = True, process_for_graph: bool = True) -> object:
        # init the return value
        ret_val = None

        # does the output directory exist
        if not os.path.isdir(data_directory):
            ret_val = Exception("Working directory does not exist. Aborting.")
        else:
            # insure the working directory ends with a '/' in order to properly append a data file name
            if data_directory[-1] != '/':
                data_directory = f'{data_directory}/'

            # process the GTEx tissue files if requested
            if process_raw_data is True:
                ret_val: object = self.gtu.process_gtex_files(data_directory, out_file_name)
            else:
                logger.info("Raw GTEx data processing not selected.")

            # does the processed file exist
            if os.path.isfile(f'{data_directory}{out_file_name}'):
                # was the raw GTEx data processed
                if ret_val is None:
                    # load up the synonymization cache for all the variants if requested
                    if process_for_cache is True:
                        ret_val: object = self.gtu.prepopulate_variant_synonymization_cache(data_directory, out_file_name)
                    else:
                        logger.info("Cache processing not selected.")

                    # is it ok to continue
                    if ret_val is None:
                        if process_for_graph is True:
                            # call the GTEx builder to load the cache and graph database
                            ret_val: object = self.create_gtex_graph(data_directory, out_file_name, 'GTEx')
                        else:
                            logger.info("Graph node/edge processing not selected.")
                    else:
                        ret_val = Exception("Error detected in pre-processing variant synonymization. Aborting.", ret_val)
                else:
                    ret_val = Exception('Error detected in GTEx file creation. Aborting.', ret_val)
            else:
                ret_val = Exception("Error detected no processed GTEx file found. Aborting.", ret_val)

        # return to the caller
        return ret_val

    #######
    # create_gtex_graph - Parses the CSV file(s) and inserts the data into the graph DB
    #
    # param data_directory: str - the name of the directory the file is in
    # param associated_file_names: list - list of file names to process
    # param analysis_id: str - the name of the data source
    # returns: object, pass if it is none, otherwise an exception object
    #######
    def create_gtex_graph(self, data_directory: str, file_name: str, analysis_id: str) -> object:
        # init the return value
        ret_val: object = None

        # init a progress counter
        line_counter = 0

        try:
            # get the full path to the input file
            full_file_path = f'{data_directory}{file_name}'

            logger.info(f'Creating GTEx graph data elements in file: {full_file_path}')

            # open a pipe to the redis cache DB
            with WriterDelegator(self.rosetta) as graph_writer:
                # init these outside of try catch block
                curie_hgvs = None
                curie_uberon = None
                curie_ensembl = None

                # loop through the variants
                # open the file and start reading
                with open(full_file_path, 'r') as inFH:
                    # open up a csv reader
                    csv_reader = csv.reader(inFH)

                    # read the header
                    header_line = next(csv_reader)

                    # index into the array to the variant id position
                    tissue_name_index = header_line.index('tissue_name')
                    tissue_uberon_index = header_line.index('tissue_uberon')
                    hgvs_index = header_line.index('HGVS')
                    # variant_id_index = header_line.index('variant_id')
                    ensembl_id_index = header_line.index('gene_id')
                    pval_nominal_index = header_line.index('pval_nominal')
                    pval_slope_index = header_line.index('slope')

                    try:
                        # for the rest of the lines in the file
                        for line in csv_reader:
                            # increment the counter
                            line_counter += 1

                            # get the data elements
                            tissue_name = line[tissue_name_index]
                            uberon = line[tissue_uberon_index]
                            hgvs = line[hgvs_index]
                            # variant_id = line[variant_id_index]
                            ensembl = line[ensembl_id_index].split(".", 1)[0]
                            pval_nominal = line[pval_nominal_index]
                            slope = line[pval_slope_index]

                            # create curies for the various id values
                            curie_hgvs = f'HGVS:{hgvs}'
                            curie_uberon = f'UBERON:{uberon}'
                            curie_ensembl = f'ENSEMBL:{ensembl}'

                            # create variant, gene and GTEx nodes with the HGVS, ENSEMBL or UBERON expression as the id and name
                            variant_node = KNode(curie_hgvs, name=curie_hgvs, type=node_types.SEQUENCE_VARIANT)
                            gene_node = KNode(curie_ensembl, type=node_types.GENE)
                            gtex_node = KNode(curie_uberon, name=tissue_name, type=node_types.ANATOMICAL_ENTITY)

                            # call to load the each node with synonyms
                            self.rosetta.synonymizer.synonymize(variant_node)
                            self.rosetta.synonymizer.synonymize(gene_node)
                            self.rosetta.synonymizer.synonymize(gtex_node)

                            # get the SequenceVariant object filled in with the sequence location data
                            # seq_var_data = self.gtu.get_sequence_variant_obj(variant_id)

                            # add properties to the variant node
                            # variant_node.properties['sequence_location'] = [seq_var_data.build, str(seq_var_data.chrom), str(seq_var_data.pos)]

                            # for now insure that the gene node has a name after synonymization
                            # this can happen if gene is not currently in the graph DB
                            if gene_node.name is None:
                                gene_node.name = curie_ensembl

                            # get the polarity of slope to get the direction of expression.
                            # positive value increases expression, negative decreases
                            label_id, label_name = self.gtu.get_expression_direction(slope)

                            # create the edge label predicate for the gene/variant relationship
                            predicate = LabeledID(identifier=label_id, label=label_name)

                            # get a MD5 hash int of the composite hyper edge ID
                            hyper_edge_id = self.gtu.get_hyper_edge_id(uberon, ensembl, Text.un_curie(variant_node.id))

                            # set the properties for the edge
                            edge_properties = [ensembl, pval_nominal, slope, analysis_id]

                            ##########################
                            # data details are ready. write all edges and nodes to the graph DB.
                            ##########################

                            # write out the sequence variant node
                            graph_writer.write_node(variant_node)

                            # write out the gene node
                            graph_writer.write_node(gene_node)

                            # write out the anatomical gtex node
                            graph_writer.write_node(gtex_node)

                            # associate the sequence variant node with an edge to the gtex anatomy node
                            self.gtu.write_new_association(graph_writer, variant_node, gtex_node, self.variant_gtex_label, hyper_edge_id, None, True)

                            # associate the gene node with an edge to the gtex anatomy node
                            self.gtu.write_new_association(graph_writer, gene_node, gtex_node, self.gene_gtex_label, None, None, False)

                            # associate the sequence variant node with an edge to the gene node. also include the GTEx properties
                            self.gtu.write_new_association(graph_writer, variant_node, gene_node, predicate, hyper_edge_id, edge_properties, True)

                            # output some feedback for the user
                            if (line_counter % 250000) == 0:
                                logger.info(f'Processed {line_counter} variants.')
                    except Exception as e:
                        logger.error(f'Exception caught trying to process variant: {curie_hgvs}-{curie_uberon}-{curie_ensembl} at data line: {line_counter}. Exception: {e}')

        except Exception as e:
            logger.error(f'Exception caught: Exception: {e}')
            ret_val = e

        # output some final feedback for the user
        logger.info(f'Building complete. Processed {line_counter} variants.')

        # return to the caller
        return ret_val

#######
# Main - Stand alone entry point for testing
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta())

    # directory with GTEx data to process
    working_data_directory = '.'
    # working_data_directory = '/projects/stars/var/GTEx/stage/smartBag/example/GTEx/GTEx_data'

    # load up all the GTEx data
    rv = gtb.load(working_data_directory, out_file_name='gtex_sample.csv', process_raw_data=False, process_for_cache=False, process_for_graph=True)

    # check the return, output error if found
    if rv is not None:
        logger.error(rv)
        raise rv
