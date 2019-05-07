from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode, KEdge
from greent.export import BufferedWriter
from greent.util import LoggingUtil
from builder.buildmain import run
from builder.question import LabeledID
from multiprocessing import Pool
from statistics import median
from collections import namedtuple

import logging, time, csv, pickle, gzip

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class GTExBuilder(object):
    #######
    # list of chromosome number to an HGVS representation conversion
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
    def __init__(self, rosetta, debug=False):
        self.rosetta = rosetta
        self.cache = rosetta.cache
        self.gtexcatalog = rosetta.core.gtexcatalog
        self.myvariant = rosetta.core.myvariant
        self.ensembl = rosetta.core.ensembl
        self.concept_model = rosetta.type_graph.concept_model

    #######
    # create_gtex_graph - Parses a CSV file and inserts the data into the graph DB
    #######
    def create_gtex_graph(self, associated_nodes, associated_file_names, data_directory, analysis_id=None):
        # for each file to parse
        for file_name in associated_file_names:
            # get the full path to the input file
            fullFilePath = f'{data_directory}{file_name}'

            # parse the CSV file
            var_dict = self.parse_csv_data(fullFilePath)

        return 0

    #######
    # Create_gtex_graph - Parses a CSV file and inserts the data into the graph DB
    # The row header, and a row of data:
    # tissue_name,            tissue_uberon,  variant_id,         gene_id,            tss_distance,   ma_samples, ma_count,   maf,        pval_nominal,   slope,      slope_se, pval_nominal_threshold,   min_pval_nominal,   pval_beta
    # Heart Atrial Appendage, 0006618,        1_1440550_T_C_b37,  ENSG00000225630.1,  875530,         12,         13,         0.0246212,  2.29069e-05,    0.996346,   0.230054, 4.40255e-05,              2.29069e-05,        0.0353012
    #######
    def parse_csv_data(self, file_path):
        # init the return
        var_dict = {}

        # open the file and start reading
        # we are looking for the folloing data points
        #
        with open(file_path, 'r') as inFH:
            # open up a csv reader
            csv_reader = csv.reader(inFH)

            # read the header
            headerLine = next(csv_reader)

            # index into the array to the variant id position
            variant_id_index = headerLine.index('variant_id')

            # for the rest of the lines in the file
            for line in csv_reader:
                # get the variant id element
                variant_id = line[variant_id_index]

                # get the HGVS expression from the variant id
                hgvs = self.convert_varid_to_hgvs(variant_id.split('_'))

                # add the pertinent data to the output array
                var_dict[hgvs] = {}

                # put away the pertinent elements needed to create a graph node

        return var_dict

    #######
    # convert_varid_to_hgvs - Converts the GTEx variant ID to HGVS nomenclature
    # The variant id layout is:
    # chr, position, ref, alt, hg version
    # 1_1440550_T_C_b37
    #######
    def convert_varid_to_hgvs(self, variant_id):
        try:
            # get indexes into the data elements
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
            logger.warning(f'Reference chromosome and/or version not found: {reference_genome}.{reference_patch},{chromosome}')
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
                # get the lenght of the insertion
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

        # layout the final HGVS expression
        hgvs = f'{ref_chromosome}:g.{variation}'

        # return the expression to the caller
        return hgvs

    #######
    # write_new_association - Writes an association edge into the graph DB
    #######
    def write_new_association(self, source_node, associated_node, predicate, p_value, analysis_id=None, node_exists=False):
        return 0

    #######
    # populate the redis cache with the baseline info
    #######
    def prepopulate_gtex_catalog_cache(self):
        self.gtexcatalog.prepopulate_gtex_catalog_cache()
        return None

    #######
    # populate the variant synomization cache
    #######
    def prepopulate_variant_synonymization_cache(self, variant_dict):
        # walk through and batch synonymize any variants that need it
        uncached_variants = []
        for chromosome, position_dict in variant_dict.items():
            for position, variants in position_dict.items():
                for variant in variants:
                    if self.cache.get(f'synonymize(HGVS:{variant.hgvs})') is None:
                        uncached_variants.append(variant.hgvs)

                    if len(uncached_variants) == 10000:
                        self.process_variant_synonymization_cache(uncached_variants)
                        uncached_variants = []

        if uncached_variants:
            self.process_variant_synonymization_cache(uncached_variants)

    #######
    # populate the variant annotation cache
    #######
    def prepopulate_variant_annotation_cache(self, batch_of_nodes):
        batch_annotations = self.myvariant.batch_sequence_variant_to_gene(batch_of_nodes)
        with self.cache.redis.pipeline() as redis_pipe:
            for seq_var_curie, annotations in batch_annotations.items():
                key = f'myvariant.sequence_variant_to_gene({seq_var_curie})'
                redis_pipe.set(key, pickle.dumps(annotations))
            redis_pipe.execute()

    #######
    # TODO: prepopulate_gtexcatalog_cache - Populates the redis cache DB
    #######
    def prepopulate_gtexcatalog_cache(self, batch_of_nodes):
        self.gtexcatalog.prepopulate_cache()

        #batch_annotations = self.myvariant.batch_sequence_variant_to_gene(batch_of_nodes)
        #
        #with self.cache.redis.pipeline() as redis_pipe:

        #    for seq_var_curie, annotations in batch_annotations.items():

        #        key = f'myvariant.sequence_variant_to_gene({seq_var_curie})'

        #        redis_pipe.set(key, pickle.dumps(annotations))
        #    redis_pipe.execute()

        return None

#######
# Main - Stand alone entry point
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta(), debug=True)

    # load the redis cache
    gtb.prepopulate_gtexcatalog_cache(None)

    # directory with GTEx data to process
    gtex_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'

    # create a node
    gtex_id = '0002190' # ex. uberon "Adipose Subcutaneous"
    gtex_node = KNode(gtex_id, name='gtex_tissue', type=node_types.ANATOMICAL_ENTITY)

    # assign the node to an array
    associated_nodes = [gtex_node]

    # assign the name of the GTEx data file
    associated_file_names = {'little_signif.csv'}

    # call the GTEx builder
    gtb.create_gtex_graph(associated_nodes, associated_file_names, gtex_directory, 'testing_gtex')
