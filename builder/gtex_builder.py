from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode
from greent.graph_components import KEdge

from greent.export import BufferedWriter
from greent.util import LoggingUtil
from greent.util import Text

from builder.question import LabeledID

from collections import namedtuple

import hashlib
import csv
import pickle
import time

# declare a logger...
import logging
# ... and initialize it
logger = LoggingUtil.init_logging(__name__, logging.DEBUG)


##############
# class: GTExBuilder
# by Phil Owen
# desc: Class that pre-loads significant GTEx data elements into the redis cache and neo4j graph database.
##############
class GTExBuilder(object):
    # object to store the details for a variant
    SequenceVariant = namedtuple('SequenceVariant', ['build', 'chrom', 'pos', 'ref', 'alt', 'hgvs', 'node'])
    SequenceVariant.__new__.__defaults__ = (None, None)

    # object to store GTEx details of the variant
    GTExVariant = namedtuple('GTEx', ['tissue_name', 'uberon', 'ensembl', 'pval_nominal', 'slope'])
    GTExVariant.__new__.__defaults__ = (None, None)

    # object to store data parsing objects
    DataParsingResults = namedtuple('data_parsing', ['SequenceVariant', 'GTExVariant'])
    DataParsingResults.__new__.__defaults__ = (None, None)

    #######
    # list of chromosome number to an HGVS assemply conversions (hg37, hg38)
    #######
    reference_chrom_labels = {
        'b37': {
            'p1': {
                1: 'NC_000001.10',
                2: 'NC_000002.11',
                3: 'NC_000003.11',
                4: 'NC_000004.11',
                5: 'NC_000005.9',
                6: 'NC_000006.11',
                7: 'NC_000007.13',
                8: 'NC_000008.10',
                9: 'NC_000009.11',
                10: 'NC_000010.10',
                11: 'NC_000011.9',
                12: 'NC_000012.11',
                13: 'NC_000013.10',
                14: 'NC_000014.8',
                15: 'NC_000015.9',
                16: 'NC_000016.9',
                17: 'NC_000017.10',
                18: 'NC_000018.9',
                19: 'NC_000019.9',
                20: 'NC_000020.10',
                21: 'NC_000021.8',
                22: 'NC_000022.10',
                23: 'NC_000023.10',
                24: 'NC_000024.9'
            }
        },
        'b38': {
            'p1': {
                1: 'NC_000001.11',
                2: 'NC_000002.12',
                3: 'NC_000003.12',
                4: 'NC_000004.12',
                5: 'NC_000005.10',
                6: 'NC_000006.12',
                7: 'NC_000007.14',
                8: 'NC_000008.11',
                9: 'NC_000009.12',
                10: 'NC_000010.11',
                11: 'NC_000011.10',
                12: 'NC_000012.12',
                13: 'NC_000013.11',
                14: 'NC_000014.9',
                15: 'NC_000015.10',
                16: 'NC_000016.10',
                17: 'NC_000017.11',
                18: 'NC_000018.10',
                19: 'NC_000019.10',
                20: 'NC_000020.11',
                21: 'NC_000021.9',
                22: 'NC_000022.11',
                23: 'NC_000023.11',
                24: 'NC_000024.10'
            }
        }
    }

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
    # create_gtex_graph - Parses a CSV file and inserts the data into the graph DB
    # The process will go something like this:
    #   Parse the CSV file of significant variant/gene pairs
    #       Create array of SequenceVariant objects
    #   For each SequenceVarinat object
    #       Pre-populate the variant synonymization cache in redis
    #   With the redis pipeline writer
    #       For each chromosome/variant position/variant
    #           Create a KNode for the sequence variant
    #           Synonymize the Sequence variant using the HGVS expression
    #           Write out the synonymized node to neo4j
    #           Create a node label and save it
    #
    #
    #######
    def create_gtex_graph(self, data_directory, file_names, analysis_id=None):
        # for each file to parse
        for file_name in file_names:
            # get the full path to the input file
            full_file_path = f'{data_directory}{file_name}'

            # parse the CSV file to get the gtex variants into a array of SequenceVariant objects
            # this is a big file. not sure if we will run out of mem turning into objects
            gtex_var_dict = self.parse_csv_data(full_file_path)

            # init arrays and counters for data element processing
            uncached_variant_annotation_nodes = []
            redis_counter = 0

            # create static edge labels for variant/gtex and gene/gtex edges
            variant_gtex_label = LabeledID(identifier=f'GTEx:affects_expression_in', label=f'affects expression in')
            gene_gtex_label = LabeledID(identifier=f'gene_to_expression_site_association', label=f'gene to expression site association')

            # open a pipe to the redis cache DB
            with BufferedWriter(self.rosetta) as graph_writer, self.cache.redis.pipeline() as redis_pipe:
                # loop through the variants
                for chromosome, position_dict in gtex_var_dict.items():
                    # for each position in the chromosome
                    for position, variants in position_dict.items():
                        # for each variant at the position
                        # note that the "variant" element is actually an array consisting of
                        # SequenceVariant obj, GTExVariant obj
                        for var_data_obj in variants:
                            # give the data elements better names for readability
                            sequence_variant = var_data_obj.SequenceVariant
                            gtex_details = var_data_obj.GTExVariant

                            # create curies for the various id values
                            curie_hgvs = f'HGVS:{sequence_variant.hgvs}'
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
                            if float(gtex_details.slope) > 0.0:
                                label_id = f'GTEx:increases_expression_of'
                                label_name = f'increases expression'
                            else:
                                label_id = f'GTEx:decreases_expression_of'
                                label_name = f'decreases expression'

                            # create the edge label predicate for the gene/variant relationship
                            predicate = LabeledID(identifier=label_id, label=label_name)

                            # create a composite hyper edge id. the components of the composite are: (in this order):
                            # <uberon tissue id>_<ensemble gene id>_<variant CAID id>
                            composite_id = str.encode(f'{gtex_details.uberon}_{gtex_details.ensembl}_{Text.un_curie(variant_node.id)}')

                            # now MD5 hash the encoded string and turn it into an int
                            hyper_egde_id = int(hashlib.md5(composite_id).hexdigest()[:8], 16)

                            # set the properties for the edge
                            edge_properties = [gtex_details.ensembl, gtex_details.pval_nominal, gtex_details.slope, analysis_id]

                            # associate the variant node with an edge to the gene node
                            self.write_new_association(graph_writer, variant_node, gene_node, predicate, hyper_egde_id, edge_properties)

                            # associate the sequence variant node with an edge to the gtex node
                            self.write_new_association(graph_writer, variant_node, gtex_node, variant_gtex_label, hyper_egde_id, None)

                            # associate the gene node with an edge to the gtex node
                            self.write_new_association(graph_writer, gene_node, gtex_node, gene_gtex_label, hyper_egde_id, None)

                            # check if the key doesnt exist in the cache, add it to buffer for batch loading later
                            if self.cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})') is None:
                                uncached_variant_annotation_nodes.append(variant_node)

                            # setup for a get on nearby genes from the ensembl cache
                            nearby_cache_key = f'ensembl.sequence_variant_to_gene({variant_node.id})'

                            # execute the query to get the nearby sequence var/gene from the ensembl cache
                            cached_nearby_genes = self.cache.get(nearby_cache_key)

                            # did we not find it write it out to the ensembl cache
                            if cached_nearby_genes is None:
                                # get the nearby gene from the query results
                                nearby_genes = self.ensembl.sequence_variant_to_gene(variant_node)

                                # set the info into the redis pipeline writer
                                redis_pipe.set(nearby_cache_key, pickle.dumps(nearby_genes))

                                # increment the record counter
                                redis_counter += 1

                            # if we reached a good count on the pending nearby gene records execute redis
                            # if redis_counter == 2000:
                            redis_pipe.execute()

                        # if we reached a good count on the pending variant to gene records execute redis
                        if len(uncached_variant_annotation_nodes) > 0:  # 1000:
                            self.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)

                            # clear for the next variant group
                            uncached_variant_annotation_nodes = []
        return 0

    #######
    # parse_csv_data - Parses a CSV file and creates a dictionary of sequence variant objects
    #
    # Ex. The row header, and an example row of data:
    # tissue_name,            tissue_uberon,  variant_id,         gene_id,            tss_distance,   ma_samples, ma_count,   maf,        pval_nominal,   slope,      slope_se, pval_nominal_threshold,   min_pval_nominal,   pval_beta
    # Heart Atrial Appendage, 0006618,        1_1440550_T_C_b37,  ENSG00000225630.1,  875530,         12,         13,         0.0246212,  2.29069e-05,    0.996346,   0.230054, 4.40255e-05,              2.29069e-05,        0.0353012
    #######
    def parse_csv_data(self, file_path):
        # init the return
        variant_dictionary = {}

        # open the file and start reading
        with open(file_path, 'r') as inFH:
            # open up a csv reader
            csv_reader = csv.reader(inFH)

            # read the header
            header_line = next(csv_reader)

            # index into the array to the variant id position
            tissue_name_index = header_line.index('tissue_name')
            tissue_uberon_index = header_line.index('tissue_uberon')
            variant_id_index = header_line.index('variant_id')
            ensembl_id_index = header_line.index('gene_id')
            pval_nominal_index = header_line.index('pval_nominal')
            pval_slope_index = header_line.index('slope')

            # for the rest of the lines in the file
            for line in csv_reader:
                # get the data elements
                tissue_name = line[tissue_name_index]
                uberon = line[tissue_uberon_index]
                variant_id = line[variant_id_index]
                ensembl = line[ensembl_id_index]
                pval_nominal = line[pval_nominal_index]
                slope = line[pval_slope_index]

                # create the GTEx data object
                gtex_data = self.GTExVariant(tissue_name, uberon, ensembl.split('.', 1)[0], pval_nominal, slope)

                # get the SequenceVariant object filled in with the HGVS value
                seq_var_data = self.get_sequence_variant_obj(variant_id.split('_'))

                # load the needed data into an object array of the two types
                results = self.DataParsingResults(seq_var_data, gtex_data)

                # do we have this chromosome in the array
                if seq_var_data.chrom not in variant_dictionary:
                    variant_dictionary[seq_var_data.chrom] = {}

                # do we have the position in the array
                if seq_var_data.pos not in variant_dictionary[seq_var_data.chrom]:
                    variant_dictionary[seq_var_data.chrom][seq_var_data.pos] = []

                # put away the pertinent elements needed to create a graph node
                variant_dictionary[seq_var_data.chrom][seq_var_data.pos].append(results)

        # return the array to the caller
        return variant_dictionary

    #######
    # get_sequence_variant_obj - Creates a SequenceVariant object out of the variant id data field.
    # this also converts the variant_id to a HGVS expression along the way
    #
    # The variant id layout is:
    # chr, position, ref, alt, hg version
    # ex: 1_1440550_T_C_b37
    #######
    def get_sequence_variant_obj(self, variant_id):
        try:
            # get position indexes into the data element
            reference_patch = 'p1'
            position = int(variant_id[1])
            ref_allele = variant_id[2]
            alt_allele = variant_id[3]
            reference_genome = variant_id[4]
            chromosome = int(variant_id[0])

            # X or Y to integer values for proper indexing
            if chromosome == 'X':
                chromosome = 23
            elif chromosome == 'Y':
                chromosome = 24

            # get the HGVS chromosome label
            ref_chromosome = self.reference_chrom_labels[reference_genome][reference_patch][chromosome]
        except KeyError:
            logger.warning(f'Reference chromosome and/or version not found: {variant_id}')
            return ''

        # get the length of the reference allele
        len_ref = len(ref_allele)

        # is there an alt allele
        if alt_allele == '.':
            # deletions
            if len_ref == 1:
                variation = f'{position}del'
            else:
                variation = f'{position}_{position + len_ref - 1}del'

        elif alt_allele.startswith('<'):
            # we know about these but don't support them yet
            return ''

        else:
            # get the length of the alternate allele
            len_alt = len(alt_allele)

            # if this is a SNP
            if (len_ref == 1) and (len_alt == 1):
                # simple layout of ref/alt SNP
                variation = f'{position}{ref_allele}>{alt_allele}'
            # if the alternate allele is larger than the reference is an insert
            elif (len_alt > len_ref) and alt_allele.startswith(ref_allele):
                # get the length of the insertion
                diff = len_alt - len_ref

                # get the position offset
                offset = len_alt - diff

                # layout the insert
                variation = f'{position + offset - 1}_{position + offset}ins{alt_allele[offset:]}'
            # if the reference is larger than the deletion it is a deletion
            elif (len_ref > len_alt) and ref_allele.startswith(alt_allele):
                # get the length of the deletion
                diff = len_ref - len_alt

                # get the position offset
                offset = len_ref - diff

                # if the diff is only 1 BP
                if diff == 1:
                    # layout the SNP deletion
                    variation = f'{position + offset}del'
                # else this is more that a single BP deletion
                else:
                    # layout the deletion
                    variation = f'{position + offset}_{position + offset + diff - 1}del'
            # we do not support this allele
            else:
                logger.warning(f'Format of variant not recognized for hgvs conversion: {ref_allele} to {alt_allele}')
                return ''

        # layout the final HGVS expression in curie format
        hgvs: str = f'{ref_chromosome}:g.{variation}'

        # create the sequence_variant object
        seq_var = self.SequenceVariant(reference_genome, chromosome, position, ref_allele, alt_allele, hgvs=hgvs, node=None)

        # return the expression to the caller
        return seq_var

    #######
    # write_new_association - Writes an association edge with properties into the graph DB
    #######
    def write_new_association(self, writer, source_node, associated_node, predicate, hyper_egde_id, properties=None):
        # if the concept model is loaded standardize the predicate label
        if self.concept_model:
            standard_predicate = self.concept_model.standardize_relationship(predicate)
        else:
            logger.warning('GTEx builder: concept_model was missing, predicate standardization failed')
            standard_predicate = predicate

        # assign the this parser as the data provider
        provided_by = 'GTEx'

        # create a property with the ensembl, p-value and slope, hyper edge id and namespace values
        if properties is not None:
            props = {'hyper_edge_id': hyper_egde_id, 'ENSEMBL': properties[0], 'p-value': float(properties[1]), 'slope': float(properties[2]), 'namespace': properties[3]}
        else:
            props = {'hyper_edge_id': hyper_egde_id}

        # get a timestamp
        c_time = time.time()

        # create the edge
        new_edge = KEdge(source_id=source_node.id,
                         target_id=associated_node.id,
                         provided_by=provided_by,
                         ctime=c_time,
                         original_predicate=predicate,
                         standard_predicate=standard_predicate,
                         input_id=source_node.id,
                         publications=None,
                         url=None,
                         properties=props)

        # write out the new edge
        writer.write_edge(new_edge)

        # return the edge
        return new_edge

    #######
    # populate the redis cache with the baseline info
    #######
    def prepopulate_gtex_catalog_cache(self):
        self.gtex.prepopulate_gtex_catalog_cache()
        return None

    #######
    # prepopulate_variant_synonymization_cache - populate the variant synomization cache by walking through the variant list
    # and batch synonymize any that need it
    #######
    def prepopulate_variant_synonymization_cache(self, variant_dict):
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
                    if self.cache.get(f'synonymize(HGVS:{variant[0].hgvs})') is None:
                        uncached_variants.append(variant[0].hgvs)

                    # if there is enough in the batch process it
                    if len(uncached_variants) == 10000:
                        self.process_variant_synonymization_cache(uncached_variants)

                        # clear out the bucket
                        uncached_variants = []

        # process any that are in the last batch
        if uncached_variants:
            self.process_variant_synonymization_cache(uncached_variants)

    #######
    # process_variant_synonymization_cache - processes an array of un-cached variant nodes.
    #######
    def process_variant_synonymization_cache(self, batch_of_hgvs):
        # get the incoming list of HGVS variants synomynized against clingen
        batch_synonyms = self.clingen.get_batch_of_synonyms(batch_of_hgvs)

        # open a connection to the redis cache
        with self.cache.redis.pipeline() as redis_pipe:
            # init a counter
            count = 0

            # for each clingen synonymized record
            for hgvs_curie, synonyms in batch_synonyms.items():

                # get the key
                key = f'synonymize({hgvs_curie})'

                # set the key and data to process
                redis_pipe.set(key, pickle.dumps(synonyms))

                # increment the counter
                count += 1

                # init an array of SNP IDs
                dbsnp_labled_ids = []

                # init the CA label id
                caid_labled_id = None

                # for each synonymized entry
                for syn in synonyms:
                    # is this a DBSNP id, add it to the list
                    if syn.identifier.startswith('DBSNP'):
                        dbsnp_labled_ids.append(syn)
                    # is this a CA id, set the CA label
                    elif syn.identifier.startswith('CAID'):
                        caid_labled_id = syn

                # if we got a CA id
                if caid_labled_id:
                    # clear out the synonyms' CA label
                    synonyms.remove(caid_labled_id)

                    # set the CA id and new synonym
                    redis_pipe.set(f'synonymize({caid_labled_id.identifier})', pickle.dumps(synonyms))

                    # add the new label to the list
                    synonyms.add(caid_labled_id)

                    # increment the counter
                    count += 1

                # for each DBSNP synonym
                for dbsnp_labled_id in dbsnp_labled_ids:
                    # clear out the synonyms
                    synonyms.remove(dbsnp_labled_id)

                    # set the new key
                    redis_pipe.set(f'synonymize({dbsnp_labled_id.identifier})', pickle.dumps(synonyms))

                    # add it to the cache
                    synonyms.add(dbsnp_labled_id)

                    # increment the counter
                    count += 1

                # did we reach a batch process limit
                if count == 8000:
                    # execute the pending synonym
                    redis_pipe.execute()

                    # reset the counter
                    count = 0

            # process the remainder queued up
            if count > 0:
                redis_pipe.execute()

    #######
    # populate the variant annotation cache in redis
    #######
    def prepopulate_variant_annotation_cache(self, batch_of_nodes):
        # get the list of batch operations
        batch_annotations = self.myvariant.batch_sequence_variant_to_gene(batch_of_nodes)

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


#######
# Main - Stand alone entry point
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta())

    # load the redis cache with GTEx data
    gtb.prepopulate_gtex_catalog_cache()

    # directory with GTEx data to process
    gtex_data_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'

    # assign the name of the GTEx data file
    associated_file_names = {'test_signif_Adrenal_Gland.csv'}
    # associated_file_names = {'signif_variant_gene_pairs.csv'}

    # call the GTEx builder to load the cache and graph database
    gtb.create_gtex_graph(gtex_data_directory, associated_file_names, 'GTEx')
