from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import LoggingUtil
from greent.graph_components import KEdge
from collections import namedtuple

import hashlib
import time
import pickle
import csv

# from greent import node_types
# import requests
# import json

# declare a logger...
import logging
# ... and initialize it
logger = LoggingUtil.init_logging(__name__, logging.DEBUG)


#############
# Class: GTEX(service)
# By: Phil Owen
# Date: 5/21/2019
# Desc: A service that interfaces with the GTEx SmartAPI to gather significant variant/gene expression
#       effects on tissues and inserts them into the graph DB on demand.
#############
class GTEx(Service):
    ########
    # constructor
    ########
    def __init__(self, context):
        super(GTEx, self).__init__("gtex", context)
        self.rosetta = context.rosetta

        # create static edge labels for variant/gtex and gene/gtex edges
        self.variant_gtex_label = LabeledID(identifier=f'GTEx:affects_expression_in', label=f'affects expression in')
        self.gene_gtex_label = LabeledID(identifier=f'gene_to_expression_site_association', label=f'gene to expression site association')

    ########
    # define the variant/gene relationship
    # param: KNode variant node, gene node
    ########
    def sequence_variant_to_gene(self, variant_node):
        # check the input parameters
        if variant_node is None or not isinstance(variant_node, KNode):
            logger.error('Error: Missing or invalid input variant node argument')
            return None

        # declare the return value
        ret_val = []

        # make the call to get the data from the SmartBag API

        # loop through the returned data

        # call to load the each node with synonyms
        self.rosetta.synonymizer.synonymize(variant_node)

        # create a predicate label

        # return to the caller
        return ret_val

    ########
    # Retrieve the variant/anatomy relationship
    #   check for valid input params
    #   convert sequence variant HGVS expression to a GTEx variant id expression
    #   call the GTEx API web service
    #   for each significant variant returned
    #
    # param: KNode variant_node, gtex anatomy node
    ########
    def sequence_variant_to_anatomy(self, variant_node):
        # check the input parameters
        if variant_node is None or not isinstance(variant_node, KNode):
            logger.error('Error: Missing or invalid input variant node argument')
            return None

        # call to load the each node with synonyms
        self.context.synonymizer.synonymize(variant_node)

        return None

    ########
    # define the gene/anatomy relationship
    # param: KNode gene node, gtex anatomy node
    ########
    def gene_to_anatomy(self, gene_node):
        # check the input parameters
        if gene_node is None or not isinstance(gene_node, KNode):
            logger.error('Error: Missing or invalid input gene node argument')
            return None

        # call to load the each node with synonyms
        self.rosetta.synonymizer.synonymize(gene_node)

        return None


#############
# Class: GTExUtils
# By: Phil Owen
# Date: 5/21/2019
# Desc: A class that has a number of shared static functions between the GTEx service and builder.
#############
class GTExUtils:
    ########
    # constructor
    ########
    def __init__(self, rosetta):
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
    # positive value increases expression, negative decreases
    #################
    @staticmethod
    def get_expression_direction(slope):
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
    # <uberon tissue id>_<ensemble gene id>_<variant CAID id>
    #################
    @staticmethod
    def get_hyper_edge_id(uberon, ensembl, variant):
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
    # this also converts the variant_id to a HGVS expression along the way
    #
    # The variant id layout is:
    # chr, position, ref, alt, hg version
    # ex: 1_762345_A_G_b37 becomes NC_000001.10:g.762345A>G
    #######
    def get_sequence_variant_obj(self, gtex_variant_id):
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
    #######
    def write_new_association(self, writer, source_node, associated_node, predicate, hyper_edge_id, properties=None, force_create=False):
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
    # prepopulate_variant_synonymization_cache - populate the variant synomization cache by walking through the variant list
    # and batch synonymize any that need it
    #######
    def prepopulate_variant_synonymization_cache(self, data_directory, file_names):
        logger.info("Starting variant synonymization cache prepopulation")

        # create an array to bucket the unchached variants
        uncached_variants = []

        # init a line counter
        line_counter = 0

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

                    # output some feedback for the user
                    if (line_counter % 100000) == 0:
                        logger.info(f'Processed {line_counter} variants.')

            # process any that are in the last batch
            if uncached_variants:
                self.process_variant_synonymization_cache(uncached_variants)

        logger.info(f'Variant synonymization cache prepopulation complete. Processed: {line_counter} variants.')

    #######
    # process_variant_synonymization_cache - processes an array of un-cached variant nodes.
    #######
    def process_variant_synonymization_cache(self, batch_of_hgvs):
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

    #######
    # populate the variant annotation cache in redis
    #######
    def prepopulate_variant_annotation_cache(self, batch_of_nodes):
        logger.info("Starting variant annotation cache prepopulation.")

        # get the list of batch operations
        batch_annotations = self.myvariant.batch_sequence_variant_to_gene(batch_of_nodes)

        if batch_annotations is not None:
            # get a reference to redis
            with self.cache.redis.pipeline() as redis_pipe:
                # for each records to process
                for seq_var_curie, annotations in batch_annotations.items():
                    # set the request using the CA curie
                    key = f'myvariant.sequence_variant_to_gene({seq_var_curie})'

                    # set the commands
                    redis_pipe.set(key, pickle.dumps(annotations))

                # execute the redis commands
            redis_pipe.execute()

        logger.info("Variant annotation cache prepopulating complete.")
