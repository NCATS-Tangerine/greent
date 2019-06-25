from greent.rosetta import Rosetta
from greent.graph_components import KNode
from greent.graph_components import LabeledID
from greent.export import BufferedWriter
from greent.util import LoggingUtil
from greent.graph_components import KEdge
from collections import namedtuple
import hashlib
import time
import pickle
import csv
import os

# declare a logger and initialize it
import logging
logger = LoggingUtil.init_logging("robokop-interfaces.builder.GTExUtils", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')


#############
# Class: GTExUtils
# By: Phil Owen
# Date: 5/21/2019
# Desc: A class that has a number of shared static functions for the GTEx builder.
#############
class GTExUtils:
    ########
    # Constructor
    # param rosetta : Rosetta - project obcject for shared objects
    ########
    def __init__(self, rosetta: Rosetta):
        # load DB connections, etc. from the rosetta object
        self.rosetta = rosetta
        self.myvariant = rosetta.core.myvariant
        self.cache = rosetta.cache
        self.clingen = rosetta.core.clingen
        self.concept_model = rosetta.type_graph.concept_model

        # object to store the details for a variant
        self.SequenceVariant = namedtuple('sequencevariant', ['build', 'chrom', 'pos', 'ref', 'alt', 'node'])
        self.SequenceVariant.__new__.__defaults__ = (None, None)

        # object to store GTEx details of the variant
        self.GTExVariant = namedtuple('gtexvariant', ['tissue_name', 'uberon', 'hgvs', 'ensembl', 'pval_nominal', 'slope'])
        self.GTExVariant.__new__.__defaults__ = (None, None)

        # object to store data parsing objects
        self.DataParsingResults = namedtuple('data_parsing', ['SequenceVariant', 'GTExVariant'])
        self.DataParsingResults.__new__.__defaults__ = (None, None)

    #################
    # get_expression_direction() - get the polarity of slope to get the direction of expression.
    #                              positive value increases expression, negative decreases
    # param slope: str - the float value to determine dirstion of expression
    # return (str, str) - a label ID name
    #################
    @staticmethod
    def get_expression_direction(slope: str) -> (str, str):
        try:
            # get the polarity of slope to get the direction of expression.
            # positive value increases expression, negative decreases
            if float(slope) > 0.0:
                label_id = f'GTEx:increases_expression_of'
                label_name = f'increases expression'
            else:
                label_id = f'GTEx:decreases_expression_of'
                label_name = f'decreases expression'

            # return to the caller
            return label_id, label_name
        except Exception as e:
            logger.error(e)
            return None, None

    #################
    # get_hyper_edge_id() - create a MD5 hash int of a hyper edge ID using the composite string:
    #                       <uberon tissue id>_<ensemble gene id>_<variant CAID id>
    # param uberon: str - the uberon ID
    # param ensembl: str - the ensembl ID
    # param variant: str - the variant ID
    # return hyper_egde_id : int - the hyper edge ID composite
    #################
    @staticmethod
    def get_hyper_edge_id(uberon: str, ensembl: str, variant: str) -> int:
        # check the input parameters
        if uberon is None or ensembl is None or variant is None:
            hyper_egde_id = 0
        else:
            # create a composite hyper edge id. the components of the composite are: (in this order):
            # <uberon tissue id>_<ensemble gene id>_<variant CAID id>
            composite_id = str.encode(f'{uberon}_{ensembl}_{variant}')

            # now MD5 hash the encoded string and turn it into an int
            hyper_egde_id = int(hashlib.md5(composite_id).hexdigest()[:8], 16)

        # return to the caller
        return hyper_egde_id

    #######
    # get_sequence_variant_obj - Creates a SequenceVariant object out of the variant id data field.
    #                            this also converts the variant_id to a HGVS expression along the way
    #       The variant id layout is:
    #           chr, position, ref, alt, hg version
    #           ex: 1_762345_A_G_b37 becomes NC_000001.10:g.762345A>G
    # param gtex_variant_id : str - the variant ID from the raw GTEx data
    # return SequenceVariant : namedtuple - elements extracted from the GTEx variant id
    #######
    def get_sequence_variant_obj(self, gtex_variant_id: str) -> namedtuple:
        # init the variant id storage
        variant_id = None

        try:
            # split the string into the components
            variant_id = gtex_variant_id.split('_')

            # get position indexes into the data element
            chromosome = variant_id[0]
            position = int(variant_id[1])
            ref_allele = variant_id[2]
            alt_allele = variant_id[3]
            reference_genome = variant_id[4]

            # X or Y to integer values for proper indexing
            if chromosome == 'X':
                chromosome = 23
            elif chromosome == 'Y':
                chromosome = 24
            else:
                chromosome = int(variant_id[0])
        except KeyError:
            logger.warning(f'Reference chromosome and/or version not found: {variant_id}')
            return ''

        # create the sequence_variant object
        seq_var = self.SequenceVariant(reference_genome, chromosome, position, ref_allele, alt_allele, node=None)

        # return the expression to the caller
        return seq_var

    #######
    # write_new_association - Writes an association edge with properties into the graph DB
    # param writer: BufferedWriter - writer for the edge information
    # param source_node: KNode - source node data object
    # param associated_node : KNode - associated node data object
    # param predicate : LabeledID - object with ID and label for the edge
    # param hyper_edge_id : int - composite hyper edge ID
    # param properties : list = None - edge data properties
    # param force_create : bool = False) - forces the creation of the node edge even if exists
    # return KEdge - node to node edge created
    #######
    def write_new_association(self, writer: BufferedWriter, source_node: KNode, associated_node: KNode, predicate: LabeledID, hyper_edge_id: int, properties: list = None, force_create: bool = False) -> KEdge:
        # if the concept model is loaded standardize the predicate label
        if self.concept_model:
            standard_predicate = self.concept_model.standardize_relationship(predicate)
        else:
            logger.warning('GTEx Utils: concept_model was missing, predicate standardization failed')
            standard_predicate = predicate

        # assign the this parser as the data provider
        provided_by = 'GTEx'

        # create a property with the ensembl, p-value and slope, hyper edge id and namespace values
        if properties is not None:
            props = {'ENSEMBL': properties[0], 'p-value': float(properties[1]), 'slope': float(properties[2]), 'namespace': properties[3]}
        else:
            props = {}

        # get a timestamp
        c_time = time.time()

        # create the edge
        new_edge = KEdge(source_id=source_node.id,
                         target_id=associated_node.id,
                         provided_by=provided_by,
                         ctime=c_time,
                         hyper_edge_id=hyper_edge_id,
                         original_predicate=predicate,
                         standard_predicate=standard_predicate,
                         input_id=source_node.id,
                         publications=None,
                         url=None,
                         properties=props)

        # write out the new edge
        writer.write_edge(new_edge, force_create)

        # return the edge
        return new_edge

    #######
    # prepopulate_variant_synonymization_cache - populate the variant synonymization cache by walking through the variant list
    #                                            and batch synonymize any that need it
    # param data_directory: str - the directory of the data file
    # param file_names: list - the name of the data file
    # returns : object, pass if it is none, otherwise an exception object
    #######
    def prepopulate_variant_synonymization_cache(self, data_directory: str, file_names: list) -> object:
        logger.info("Starting variant synonymization cache prepopulation")

        # init the return value
        ret_val = None

        # create an array to bucket the unchached variants
        uncached_variants = []

        # init a line counter
        line_counter = 0

        try:
            # for each file to parse
            for file_name in file_names:
                # get the full path to the input file
                full_file_path = f'{data_directory}{file_name}'

                logger.info(f'Pre-populating data elements in file: {full_file_path}')

                # open the file and start reading
                with open(full_file_path, 'r') as inFH:
                    # open up a csv reader
                    csv_reader = csv.reader(inFH)

                    # read the header
                    header_line = next(csv_reader)

                    # index into the array to the HGVS position
                    hgvs_index = header_line.index('HGVS')

                    # for the rest of the lines in the file
                    for line in csv_reader:
                        # increment the counter
                        line_counter += 1

                        try:
                            # get the HGVS data element
                            hgvs = line[hgvs_index]

                            # look up the variant by the HGVS expresson
                            if self.cache.get(f'synonymize(HGVS:{hgvs})') is None:
                                uncached_variants.append(hgvs)

                            # if there is enough in the batch process it
                            if len(uncached_variants) == 10000:
                                self.process_variant_synonymization_cache(uncached_variants)

                                # clear out the bucket
                                uncached_variants = []

                        except Exception as e:
                            logger.error(f'Exception caught at line: {line_counter}. Exception: {e}')
                            logger.error('Continuing...')

                        # output some feedback for the user
                        if (line_counter % 250000) == 0:
                            logger.info(f'Processed {line_counter} variants.')

                # process any that are in the last batch
                if uncached_variants:
                    self.process_variant_synonymization_cache(uncached_variants)
        except Exception as e:
            logger.error(f'Exception caught. Exception: {e}')
            ret_val = e

        logger.info(f'Variant synonymization cache prepopulation complete. Processed: {line_counter} variants.')

        # return to the caller
        return ret_val

    #######
    # process_variant_synonymization_cache - processes an array of un-cached variants by HGVS expression.
    # param batch_of_hgvs: list - list og HGVS expressions
    #######
    def process_variant_synonymization_cache(self, batch_of_hgvs: list):
        logger.info("Starting variant synonymization cache processing")

        # process a list of hgvs values
        batch_synonyms = self.clingen.get_batch_of_synonyms(batch_of_hgvs)

        # open up a connection to the cache database
        with self.cache.redis.pipeline() as redis_pipe:
            # init a counter
            count = 0

            # for each hgvs item returned
            for hgvs_curie, synonyms in batch_synonyms.items():
                # create a data key
                key = f'synonymize({hgvs_curie})'

                # set the key for the cache lookup
                redis_pipe.set(key, pickle.dumps(synonyms))

                # increment the counter
                count += 1

                # for each synonym
                for syn in synonyms:
                    # is this our id
                    if syn.identifier.startswith('CAID'):
                        # save the id
                        caid_labled_id = syn

                        # remove the synonym from the list
                        synonyms.remove(caid_labled_id)

                        # set the new synonymization id
                        redis_pipe.set(f'synonymize({caid_labled_id.identifier})', pickle.dumps(synonyms))

                        # add it back to the list with the new info
                        synonyms.add(caid_labled_id)

                        # increase the counter again
                        count += 1

                        # no need to continue
                        break

                # did we reach a critical count to write out to the cache
                if count == 10000:
                    # execute the statement
                    redis_pipe.execute()

                    # reset the counter
                    count = 0

            # execute any remainder entries
            if count > 0:
                redis_pipe.execute()

        logger.info("Variant synonymization cache processing complete.")
