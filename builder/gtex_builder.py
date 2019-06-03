from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode
from greent.export import BufferedWriter
from greent.util import LoggingUtil
from greent.util import Text
from greent.services.gtex import GTExUtils

from builder.question import LabeledID

import csv
import pickle


# declare a logger and initialize it.
import logging
logger = LoggingUtil.init_logging(__name__, logging.DEBUG, format='medium')


##############
# Class: GTExBuilder
# By: Phil Owen
# Date: 5/21/2019
# Desc: Class that pre-loads significant GTEx data elements into the redis cache and neo4j graph database.
##############
class GTExBuilder(object):
    # create static edge labels for variant/gtex and gene/gtex edges
    variant_gtex_label = LabeledID(identifier=f'GTEx:affects_expression_in', label=f'affects expression in')
    gene_gtex_label = LabeledID(identifier=f'gene_to_expression_site_association', label=f'gene to expression site association')

    #######
    # Constructor
    #######
    def __init__(self, rosetta):
        self.rosetta = rosetta
        self.cache = rosetta.cache
        self.clingen = rosetta.core.clingen
        self.gtex = rosetta.core.gtex
        self.myvariant = rosetta.core.myvariant
        self.ensembl = rosetta.core.ensembl
        self.concept_model = rosetta.type_graph.concept_model

    #######
    # create_gtex_graph - Parses CSV file(s) and inserts the data into the graph DB
    #
    # The process will go something like this:
    #   Parse the CSV file of significant variant/gene pairs
    #       Create array of SequenceVariant objects
    #   For each SequenceVariant object
    #       Pre-populate the variant synonymization cache in redis
    #   With the redis pipeline writer
    #       For each chromosome/variant position/variant
    #           Create a KNode for the sequence variant
    #           Synonymize the Sequence variant using the HGVS expression
    #           Write out the synonymized node to neo4j
    #           Create a hyper edge that will be used to identify a gene/anatomy/allele/gene relationship
    #           write out the node/edge relationships
    #######
    def create_gtex_graph(self, data_directory, file_names, analysis_id=None):
        # for each file to parse
        for file_name in file_names:
            # get the full path to the input file
            full_file_path = f'{data_directory}{file_name}'

            # parse the CSV file to get the gtex variants into a array of SequenceVariant objects
            # this is a big file. not sure if we will run out of mem turning into objects
            gtex_var_dict = self.parse_csv_data(full_file_path)

            # load up the synonymization cache of all the variant
            self.prepopulate_variant_synonymization_cache(gtex_var_dict)

            # init some progress counters
            var_counter = 0
            pos_counter = 0

            # open a pipe to the redis cache DB
            with BufferedWriter(self.rosetta) as graph_writer:
                # loop through the variants
                for chromosome, position_dict in gtex_var_dict.items():
                    # for each position in the chromosome
                    for position, variants in position_dict.items():
                        # increment the number of positions we have processed
                        pos_counter += 1

                        # for each variant at the position
                        # note that the "variant" element is actually an array consisting of
                        # SequenceVariant obj, GTExVariant obj
                        for var_data_obj in variants:
                            try:
                                # increment a variant counter
                                var_counter += 1

                                # give the data elements better names for readability
                                sequence_variant = var_data_obj.SequenceVariant
                                gtex_details = var_data_obj.GTExVariant

                                # create curies for the various id values
                                curie_hgvs = f'HGVS:{gtex_details.hgvs}'
                                curie_uberon = f'UBERON:{gtex_details.uberon}'
                                curie_ensembl = f'ENSEMBL:{gtex_details.ensembl}'

                                # create variant, gene and GTEx nodes with the HGVS, ENSEMBL or UBERON expression as the id and name
                                variant_node = KNode(curie_hgvs, name=curie_hgvs, type=node_types.SEQUENCE_VARIANT)
                                gene_node = KNode(curie_ensembl, type=node_types.GENE)
                                gtex_node = KNode(curie_uberon, name=gtex_details.tissue_name, type=node_types.ANATOMICAL_ENTITY)

                                # call to load the each node with synonyms
                                self.rosetta.synonymizer.synonymize(variant_node)
                                self.rosetta.synonymizer.synonymize(gene_node)
                                self.rosetta.synonymizer.synonymize(gtex_node)

                                # add properties to the variant node and write it out
                                variant_node.properties['sequence_location'] = [sequence_variant.build, str(sequence_variant.chrom), str(sequence_variant.pos)]
                                graph_writer.write_node(variant_node)

                                # for now insure that the gene node has a name after synonymization
                                # this can happen if gene is not currently in the graph DB
                                if gene_node.name is None:
                                    gene_node.name = curie_ensembl

                                # write out the gene node
                                graph_writer.write_node(gene_node)

                                # write out the anatomical gtex node
                                graph_writer.write_node(gtex_node)

                                # get the polarity of slope to get the direction of expression.
                                # positive value increases expression, negative decreases
                                label_id, label_name = GTExUtils.get_expression_direction(gtex_details.slope)

                                # create the edge label predicate for the gene/variant relationship
                                predicate = LabeledID(identifier=label_id, label=label_name)

                                # get a MD5 hash int of the composite hyper edge ID
                                hyper_egde_id = GTExUtils.get_hyper_edge_id(gtex_details.uberon, gtex_details.ensembl, Text.un_curie(variant_node.id))

                                # set the properties for the edge
                                edge_properties = [gtex_details.ensembl, gtex_details.pval_nominal, gtex_details.slope, analysis_id]

                                # associate the sequence variant node with an edge to the gtex anatomy node
                                GTExUtils.write_new_association(graph_writer, variant_node, gtex_node, self.variant_gtex_label, hyper_egde_id, self.concept_model, None, True)

                                # associate the gene node with an edge to the gtex anatomy node
                                GTExUtils.write_new_association(graph_writer, gene_node, gtex_node, self.gene_gtex_label, hyper_egde_id, self.concept_model, None)

                                # associate the sequence variant node with an edge to the gene node. also include the GTEx properties
                                GTExUtils.write_new_association(graph_writer, variant_node, gene_node, predicate, hyper_egde_id, self.concept_model, edge_properties, True)

                            except Exception as e:
                                logger.error(f'Exception caught trying to process variant: {curie_hgvs}-{curie_uberon}-{curie_ensembl} at file position: {var_counter}. Exception: {e}')
                                logger.error('Continuing...')

                    # output some feedback for the user
                    if (pos_counter % 10000) == 0:
                        logger.info(f'Processed {var_counter} variants at {pos_counter} position(s).')

                # output some final feedback for the user
                logger.info(f'Building complete. Processed {var_counter} variants at {pos_counter} position(s).')
        return 0

    #######
    # parse_csv_data - Parses a CSV file and creates a dictionary of sequence variant objects
    #
    # Ex. The row header, and an example row of data:
    # tissue_name,          tissue_uberon,  hgvs,                       variant_id,         gene_id,            tss_distance,   ma_samples,
    # Adipose Subcutaneous, 0002190,        NC_000001.10:g.753865G>C,   1_753865_G_C_b37,   ENSG00000237683.5,  614486,         12,
    # (cont.)
    # ma_count, maf,        pval_nominal,   slope,    slope_se,   pval_nominal_threshold, min_pval_nominal,   pval_beta
    # 12,       0.0159151,  4.94712e-05,    0.914962, 0.222374,   0.000132768,            4.94712e-05,        0.0448675
    #######
    @staticmethod
    def parse_csv_data(file_path):
        logger.info(f'Parsing CSV file: {file_path}')

        # init the return
        variant_dictionary = {}

        # init a data line counter
        line_counter = 0

        # open the file and start reading
        with open(file_path, 'r') as inFH:
            # open up a csv reader
            csv_reader = csv.reader(inFH)

            # read the header
            header_line = next(csv_reader)

            # index into the array to the variant id position
            tissue_name_index = header_line.index('tissue_name')
            tissue_uberon_index = header_line.index('tissue_uberon')
            hgvs_index = header_line.index('HGVS')
            variant_id_index = header_line.index('variant_id')
            ensembl_id_index = header_line.index('gene_id')
            pval_nominal_index = header_line.index('pval_nominal')
            pval_slope_index = header_line.index('slope')

            # for the rest of the lines in the file
            for line in csv_reader:
                # increment the counter
                line_counter += 1

                # get the data elements
                tissue_name = line[tissue_name_index]
                uberon = line[tissue_uberon_index]
                hgvs = line[hgvs_index]
                variant_id = line[variant_id_index]
                ensembl = line[ensembl_id_index]
                pval_nominal = line[pval_nominal_index]
                slope = line[pval_slope_index]

                # create the GTEx data object. also remove the transcript appended onto the ensembl id
                gtex_data = GTExUtils.GTExVariant(tissue_name, uberon, hgvs, ensembl.split('.', 1)[0], pval_nominal, slope)

                # get the SequenceVariant object filled in with the HGVS value
                seq_var_data = GTExUtils.get_sequence_variant_obj(variant_id)

                # load the needed data into an object array of the two types
                results = GTExUtils.DataParsingResults(seq_var_data, gtex_data)

                # do we have this chromosome in the array
                if seq_var_data.chrom not in variant_dictionary:
                    variant_dictionary[seq_var_data.chrom] = {}

                # do we have the position in the array
                if seq_var_data.pos not in variant_dictionary[seq_var_data.chrom]:
                    variant_dictionary[seq_var_data.chrom][seq_var_data.pos] = []

                # put away the pertinent elements needed to create a graph node
                variant_dictionary[seq_var_data.chrom][seq_var_data.pos].append(results)

        logger.info(f'CSV record count: {line_counter}')

        # return the array to the caller
        return variant_dictionary

    #######
    # prepopulate_variant_synonymization_cache - populate the variant synomization cache by walking through the variant list
    # and batch synonymize any that need it
    #######
    def prepopulate_variant_synonymization_cache(self, variant_dict):
        logger.info("Starting variant synonymization cache prepopulation")

        # create an array to bucket the unchached variants
        uncached_variants = []

        # go through each chromosome
        for chromosome, position_dict in variant_dict.items():
            # go through each variant position
            for position, variants in position_dict.items():
                # go through each variant at the position
                # note that the "variant" element is actually an array consisting of
                # [SequenceVariant obj, uberon id, ensembl (aka gene) id]
                for variant in variants:
                    # look up the variant by the HGVS expresson
                    if self.cache.get(f'synonymize(HGVS:{variant.GTExVariant.hgvs})') is None:
                        uncached_variants.append(variant.GTExVariant.hgvs)

                    # if there is enough in the batch process it
                    if len(uncached_variants) == 10000:
                        self.process_variant_synonymization_cache(uncached_variants)

                        # clear out the bucket
                        uncached_variants = []

        # process any that are in the last batch
        if uncached_variants:
            self.process_variant_synonymization_cache(uncached_variants)

        logger.info("Variant synonymization cache prepopulation complete.")

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
                if count == 8000:
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

#######
# Main - Stand alone entry point
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta())

    # directory with GTEx data to process
    gtex_data_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'

    # assign the name of the GTEx data file
    # available test files:
    # 'signif_variant_gene_pairs',
    # 'test_signif_Adipose_Subcutaneous_all', 'test_signif_Adipose_Subcutaneous_100k', 'test_signif_Adipose_Subcutaneous_10k', 'test_signif_Adipose_Subcutaneous_100', 'test_signif_Adipose_Subcutaneous_6'
    # 'test_signif_Stomach_all', 'test_signif_Stomach_100k', 'test_signif_Stomach_10k', 'test_signif_Stomach_100', 'test_signif_Stomach_6'
    # 'hypertest_1-var_2-genes_1-tissue', 'hypertest_1-var_2-tissues_1-gene'
    associated_file_names = {'test_signif_Adipose_Subcutaneous_100.csv', 'test_signif_Stomach_100.csv'}

    # call the GTEx builder to load the cache and graph database
    gtb.create_gtex_graph(gtex_data_directory, associated_file_names, 'GTEx')
