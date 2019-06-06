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
logger = LoggingUtil.init_logging(__name__, logging.INFO, format='medium')


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
    # create_gtex_graph - Parses the CSV file(s) and inserts the data into the graph DB
    #
    # Ex. The row header, and an example row of data:
    # tissue_name,          tissue_uberon,  hgvs,                       variant_id,         gene_id,            tss_distance,   ma_samples,
    # Adipose Subcutaneous, 0002190,        NC_000001.10:g.753865G>C,   1_753865_G_C_b37,   ENSG00000237683.5,  614486,         12,
    # (cont.)
    # ma_count, maf,        pval_nominal,   slope,    slope_se,   pval_nominal_threshold, min_pval_nominal,   pval_beta
    # 12,       0.0159151,  4.94712e-05,    0.914962, 0.222374,   0.000132768,            4.94712e-05,        0.0448675
    #######
    def create_gtex_graph(self, data_directory, file_names, analysis_id=None):
        # for each file to parse
        for file_name in file_names:
            # get the full path to the input file
            full_file_path = f'{data_directory}{file_name}'

            logger.info(f'Creating GTEx graph data elements in file: {full_file_path}')

            # load up the synonymization cache of all the variant
            # self.prepopulate_variant_synonymization_cache(full_file_path)

            # init a progress counter
            line_counter = 0

            # get a ref to the util class
            gtu = GTExUtils()

            # open a pipe to the redis cache DB
            with BufferedWriter(self.rosetta) as graph_writer:
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
                        ensembl = line[ensembl_id_index].split(".", 1)[0]
                        pval_nominal = line[pval_nominal_index]
                        slope = line[pval_slope_index]

                        try:
                            # create curies for the various id values
                            curie_hgvs = f'HGVS:{hgvs}'
                            curie_uberon = f'UBERON:{uberon}'
                            curie_ensembl = f'ENSEMBL:{ensembl}'

                            # create variant, gene and GTEx nodes with the HGVS, ENSEMBL or UBERON expression as the id and name
                            variant_node = KNode(curie_hgvs, name=curie_hgvs, type=node_types.SEQUENCE_VARIANT)
                            gene_node = KNode(curie_ensembl, type=node_types.GENE)
                            gtex_node = KNode(curie_uberon, name=tissue_name, type=node_types.ANATOMICAL_ENTITY)

                            # # call to load the each node with synonyms
                            # self.rosetta.synonymizer.synonymize(variant_node)
                            # self.rosetta.synonymizer.synonymize(gene_node)
                            # self.rosetta.synonymizer.synonymize(gtex_node)

                            # # get the SequenceVariant object filled in with the sequence location data
                            # seq_var_data = gtu.get_sequence_variant_obj(variant_id)
                            #
                            # # add properties to the variant node and write it out
                            # variant_node.properties['sequence_location'] = [seq_var_data.build, str(seq_var_data.chrom), str(seq_var_data.pos)]
                            # graph_writer.write_node(variant_node)
                            #
                            # # for now insure that the gene node has a name after synonymization
                            # # this can happen if gene is not currently in the graph DB
                            # if gene_node.name is None:
                            #     gene_node.name = curie_ensembl
                            #
                            # # write out the gene node
                            # graph_writer.write_node(gene_node)
                            #
                            # # write out the anatomical gtex node
                            # graph_writer.write_node(gtex_node)
                            #
                            # # get the polarity of slope to get the direction of expression.
                            # # positive value increases expression, negative decreases
                            # label_id, label_name = gtu.get_expression_direction(slope)
                            #
                            # # create the edge label predicate for the gene/variant relationship
                            # predicate = LabeledID(identifier=label_id, label=label_name)
                            #
                            # # get a MD5 hash int of the composite hyper edge ID
                            # hyper_egde_id = gtu.get_hyper_edge_id(uberon, ensembl, Text.un_curie(variant_node.id))
                            #
                            # # set the properties for the edge
                            # edge_properties = [ensembl, pval_nominal, slope, analysis_id]
                            #
                            # # associate the sequence variant node with an edge to the gtex anatomy node
                            # gtu.write_new_association(graph_writer, variant_node, gtex_node, self.variant_gtex_label, hyper_egde_id, self.concept_model, None, True)
                            #
                            # # associate the gene node with an edge to the gtex anatomy node
                            # gtu.write_new_association(graph_writer, gene_node, gtex_node, self.gene_gtex_label, hyper_egde_id, self.concept_model, None)
                            #
                            # # associate the sequence variant node with an edge to the gene node. also include the GTEx properties
                            # gtu.write_new_association(graph_writer, variant_node, gene_node, predicate, hyper_egde_id, self.concept_model, edge_properties, True)

                        except Exception as e:
                            logger.error(f'Exception caught trying to process variant: {curie_hgvs}-{curie_uberon}-{curie_ensembl}. Exception: {e}')
                            logger.error('Continuing...')

                        # output some feedback for the user
                        if (line_counter % 100000) == 0:
                            logger.info(f'Processed {line_counter} variants.')

                # output some final feedback for the user
                logger.info(f'Building complete. Processed {line_counter} variants.')
        return 0

    #######
    # prepopulate_variant_synonymization_cache - populate the variant synomization cache by walking through the variant list
    # and batch synonymize any that need it
    #######
    def prepopulate_variant_synonymization_cache(self, file_path):
        logger.info("Starting variant synonymization cache prepopulation")

        # create an array to bucket the unchached variants
        uncached_variants = []

        # init a line counter
        line_counter = 0

        # open the file and start reading
        with open(file_path, 'r') as inFH:
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

#######
# Main - Stand alone entry point
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta())

    # directory with GTEx data to process
    # gtex_data_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'
    gtex_data_directory = '/projects/stars/var/GTEx/stage/smartBag/example/GTEx/bag/data/'

    # assign the name of the GTEx data file
    # available test files:
    # 'signif_variant_gene_pairs',
    # 'test_signif_Adipose_Subcutaneous_all', 'test_signif_Adipose_Subcutaneous_100k', 'test_signif_Adipose_Subcutaneous_10k', 'test_signif_Adipose_Subcutaneous_100', 'test_signif_Adipose_Subcutaneous_6'
    # 'test_signif_Stomach_all', 'test_signif_Stomach_100k', 'test_signif_Stomach_10k', 'test_signif_Stomach_100', 'test_signif_Stomach_6'
    # 'hypertest_1-var_2-genes_1-tissue', 'hypertest_1-var_2-tissues_1-gene'
    associated_file_names = {'signif_variant_gene_pairs.csv'}

    # call the GTEx builder to load the cache and graph database
    gtb.create_gtex_graph(gtex_data_directory, associated_file_names, 'GTEx')
