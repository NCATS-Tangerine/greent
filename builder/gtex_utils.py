from greent.rosetta import Rosetta
from greent.graph_components import KNode
from greent.graph_components import LabeledID
from greent.export_delegator import WriterDelegator
from greent.util import LoggingUtil
from greent.graph_components import KEdge

from collections import namedtuple
import hashlib
import time
import pickle
import csv
import os
from urllib import request
import tarfile
import gzip

# declare a logger and initialize it
import logging
logger = LoggingUtil.init_logging("robokop-interfaces.builder.GTExUtils", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')


#############
# Class: GTExUtils
#
# By: Phil Owen
# Date: 5/21/2019
# Desc: A class that has a number of shared functions for the GTEx builder class.
#############
class GTExUtils:
    ########
    # Constructor
    # param rosetta : Rosetta - project object for shared objects
    ########
    def __init__(self, rosetta: Rosetta):
        # load DB connections, etc. from the rosetta object
        self.rosetta = rosetta
        self.myvariant = rosetta.core.myvariant
        self.cache = rosetta.cache
        self.clingen = rosetta.core.clingen
        self.concept_model = rosetta.type_graph.concept_model

        # object to store the details for a variant
        self.SequenceVariant: dict = namedtuple('sequencevariant', ['build', 'chrom', 'pos', 'ref', 'alt', 'node'])
        self.SequenceVariant.__new__.__defaults__ = (None, None)

        # object to store GTEx details of the variant
        self.GTExVariant: dict = namedtuple('gtexvariant', ['tissue_name', 'uberon', 'hgvs', 'ensembl', 'pval_nominal', 'slope'])
        self.GTExVariant.__new__.__defaults__ = (None, None)

        # object to store data parsing objects
        self.DataParsingResults: dict = namedtuple('data_parsing', ['SequenceVariant', 'GTExVariant'])
        self.DataParsingResults.__new__.__defaults__ = (None, None)

        # the expected number of columns for error checking
        self.col_count: int = 12

        # list of all the tissues in the GTEX data with the uberon tissue codes
        self.tissues: dict = {
            "Adipose_Subcutaneous": "0002190",
            "Adipose_Visceral_Omentum": "0003688",
            "Adrenal_Gland": "0018303",
            "Artery_Aorta": "0004178",
            "Artery_Coronary": "0002111",
            "Artery_Tibial": "0007610",
            "Brain_Amygdala": "0001876",
            "Brain_Anterior_cingulate_cortex_BA24": "0006101",
            "Brain_Caudate_basal_ganglia": "0002420",
            "Brain_Cerebellar_Hemisphere": ",0002245",
            "Brain_Cerebellum": "0002037",
            "Brain_Cortex": "0001851",
            "Brain_Frontal_Cortex_BA9": "0013540",
            "Brain_Hippocampus": "0002310",
            "Brain_Hypothalamus": "0001898",
            "Brain_Nucleus_accumbens_basal_ganglia": "0001882",
            "Brain_Putamen_basal_ganglia": "0001874",
            "Brain_Spinal_cord_cervical_c-1": "0002726",
            "Brain_Substantia_nigra": "0002038",
            "Breast_Mammary_Tissue": "0001911",
            "Cells_Cultured_fibroblasts": "0015764",
            "Cells_EBV-transformed_lymphocytes": "0001744",
            "Colon_Sigmoid": "0001159",
            "Colon_Transverse": "0001157",
            "Esophagus_Gastroesophageal_Junction": "0007650",
            "Esophagus_Mucosa": "0002469",
            "Esophagus_Muscularis": "0004648",
            "Heart_Atrial_Appendage": "0006618",
            "Heart_Left_Ventricle": "0002084",
            "Kidney_Cortex": "0001225",
            "Liver": "0002107",
            "Lung": "0002048",
            "Minor_Salivary_Gland": "0001830",
            "Muscle_Skeletal": "0001134",
            "Nerve_Tibial": "0001323",
            "Ovary": "0000992",
            "Pancreas": "0001264",
            "Pituitary": "0000007",
            "Prostate": "0002367",
            "Skin_Not_Sun_Exposed_Suprapubic": "0036149",
            "Skin_Sun_Exposed_Lower_leg": "0004264",
            "Small_Intestine_Terminal_Ileum": "0002116",
            "Spleen": "0002106",
            "Stomach": "0000945",
            "Testis": "0000473",
            "Thyroid": "0002046",
            "Uterus": "0000995",
            "Vagina": "0000996",
            "Whole_Blood": "0000178"}

        # maps the HG version to the chromosome versions
        self.reference_chrom_labels: dict = {
            'b37': {
                'p1': {
                    1: 'NC_000001.10', 2: 'NC_000002.11', 3: 'NC_000003.11', 4: 'NC_000004.11', 5: 'NC_000005.9',
                    6: 'NC_000006.11', 7: 'NC_000007.13', 8: 'NC_000008.10', 9: 'NC_000009.11', 10: 'NC_000010.10', 11: 'NC_000011.9',
                    12: 'NC_000012.11', 13: 'NC_000013.10', 14: 'NC_000014.8', 15: 'NC_000015.9', 16: 'NC_000016.9', 17: 'NC_000017.10',
                    18: 'NC_000018.9', 19: 'NC_000019.9', 20: 'NC_000020.10', 21: 'NC_000021.8', 22: 'NC_000022.10', 23: 'NC_000023.10',
                    24: 'NC_000024.9'
                }
            },
            'b38': {
                'p1': {
                    1: 'NC_000001.11', 2: 'NC_000002.12', 3: 'NC_000003.12', 4: 'NC_000004.12', 5: 'NC_000005.10',
                    6: 'NC_000006.12', 7: 'NC_000007.14', 8: 'NC_000008.11', 9: 'NC_000009.12', 10: 'NC_000010.11', 11: 'NC_000011.10',
                    12: 'NC_000012.12', 13: 'NC_000013.11', 14: 'NC_000014.9', 15: 'NC_000015.10', 16: 'NC_000016.10', 17: 'NC_000017.11',
                    18: 'NC_000018.10', 19: 'NC_000019.10', 20: 'NC_000020.11', 21: 'NC_000021.9', 22: 'NC_000022.11', 23: 'NC_000023.11',
                    24: 'NC_000024.10'
                }
            }
        }

    #############
    # process_gtex_files - gets a reformatted GTEx data file.
    # rev .0, 5/21/2019- supports v7 of GTEx data
    # rev .1, 9/27/2019 - supports v8 of GTEx data
    #
    # param working_data_directory: str - the location of where the output files should go, defaults to the current directory
    # param out_file_name: str - the name of the processed output file, defaults to a default data file name
    # param tar_file_name: str - the name of the GTEx data file name (tar)
    # returns ret_val: object - Exception on error, otherwise None
    #############
    def process_gtex_files(self, working_data_directory: str, out_file_name: str, tar_file_name: str = 'GTEx_Analysis_v8_sQTL.tar') -> object:
        # init the return
        ret_val = None

        # init the input file path
        full_tar_path = ''

        try:
            # define full paths to the input and output files
            full_tar_path = f'{working_data_directory}{tar_file_name}'
            full_out_path = f'{working_data_directory}{out_file_name}'

            # define the url for the raw data file
            url = f'https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/{tar_file_name}'

            logger.info(f'Downloading raw GTEx data file {url}.')

            # get a http handle to the file stream
            http_handle = request.urlopen(url)

            # open the file and save it
            with open(full_tar_path, 'wb') as tar_file:
                # while there is data
                while True:
                    # read a block of data
                    data = http_handle.read(8192)

                    # if nothing read
                    if len(data) == 0:
                        break

                    # write out the data to the output file
                    tar_file.write(data)

            logger.info(f'GTEx tar file downloaded. Extracting and parsing individual tissue files in {full_tar_path}.')

            # init a first line read flag
            first_file_flag: bool = True

            # for each file in the tar archive
            with tarfile.open(full_tar_path, 'r:') as tar_files, open(full_out_path, 'w') as output_file:
                # insure that we have the correct number of expected tar_files. the contents of this file contains 2 types for each tissue
                if len(self.tissues) * 2 == len(tar_files.getnames()):
                    # for each tissue data file in the tar
                    for tissue_file in tar_files:
                        # get a handle to the tissue file
                        tissue_handle = tar_files.extractfile(tissue_file)

                        # is this a "significant variant" data file. expecting format: 'GTEx_Analysis_v8_sQTL/<tissue_name>.v8.sqtl_signifpairs.txt.gz'
                        if tissue_file.name.find('sqtl_signifpairs') > 0:
                            logger.debug(f'Processing tissue file {tissue_file.name}.')

                            # get the tissue name from the name of the file
                            tissue_name: str = tissue_file.name.split('/')[1].split('.')[0]

                            # lookup the uberon code for the tissue using the file name
                            tissue_uberon: str = self.tissues[tissue_name]

                            # check to make sure we know about this file
                            if tissue_uberon is not None:
                                # insure that the file name doesnt have an underscore ro the rest of this files' processing
                                tissue_name = tissue_name.replace('_', ' ')

                                # open up the compressed file
                                with gzip.open(tissue_handle, 'rt') as compressed_file:
                                    # get the file line of the file
                                    first_line = next(compressed_file)

                                    # if this if the first file write out the csv file header
                                    if first_file_flag is True:
                                        output_file.write(f'tissue_name,tissue_uberon,HGVS,gene_id,{first_line}'.replace('\t', ','))
                                        first_file_flag = False

                                    # for each line in the file
                                    for line in compressed_file:
                                        output_file.write(self.parse_tissue_line(line, tissue_name, tissue_uberon, 0, 1))
                            else:
                                logger.debug(f'Skipping unexpected tissue file {tissue_file.name}.')
                        else:
                            logger.debug(f'Skipping non-significant tissue file {tissue_file.name}.')
                else:
                    raise Exception(f'Unexpected number of GTEx input files detected. Aborting.')
        except Exception as e:
            logger.error(f'Exception caught. Exception: {e}')
            ret_val = e
        finally:
            # remove all the intermediate (tar) files
            if os.path.isfile(full_tar_path):
                os.remove(full_tar_path)

        logger.info(f'GTEx data file decompression and reformatting complete.')

        # return the output file name to the caller
        return ret_val

    #############
    # parse_tissue_line - parses a line of tissue csv data from a GTEx tissue file
    #
    # param line - the line of data from the tissue file
    # param tissue_name - the name of the tissue
    # param tissue_uberon - the tissue uberon id
    # returns the output line to add to the output file
    #############
    def parse_tissue_line(self, line: str, tissue_name: str, tissue_uberon: str, variant_id_index: int, phenotype_id_index: int) -> str:
        # init a line counter for error checking
        line_count: int = 1

        # init the return
        new_line: str = ''

        # convert the tabs to commas
        line = line.replace('\t', ',')

        # split line the into an array
        line_split: list = line.split(',')

        # check the column count
        if len(line_split) != self.col_count:
            print(f'Error with column count. Got:{len(line_split)}, expected {self.col_count} in {tissue_name} at position {line_count}')
        else:
            # get the variant ID value
            variant_id: str = line_split[variant_id_index]
            phenotype_id: str = line_split[phenotype_id_index]

            # get the HGVS value
            hgvs: str = self.get_hgvs_value(variant_id[3:])

            # the phenotype id contains the ensembl id for the gene.
            # it has the format: chr1:497299:498399:clu_51878:ENSG00000237094.11
            gene_id: str = phenotype_id.split(':')[4].split('.')[0]

            # prepend the input line with the tissue name and uberon id
            new_line = f'{tissue_name},{tissue_uberon},{hgvs},{gene_id},{line}'

            # increment the line counter
            line_count += 1

        # return the line to write to the output file
        return new_line

    #############
    # get_hgvs_value - parses the GTEx variant ID and converts it to an HGVS expression
    #
    # param gtex_variant_id: str - the gtex variant id
    # returns: str the HGVS value
    #############
    def get_hgvs_value(self, gtex_variant_id: str):
        try:
            # split the string into the components
            variant_id = gtex_variant_id.split('_')

            # get position indexes into the data element
            reference_patch = 'p1'
            position = int(variant_id[1])
            ref_allele = variant_id[2]
            alt_allele = variant_id[3]
            reference_genome = variant_id[4]
            chromosome = variant_id[0]

            # X or Y to integer values for proper indexing
            if chromosome == 'X':
                chromosome = 23
            elif chromosome == 'Y':
                chromosome = 24
            else:
                chromosome = int(variant_id[0])

            # get the HGVS chromosome label
            ref_chromosome = self.reference_chrom_labels[reference_genome][reference_patch][chromosome]
        except KeyError:
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
                return ''

        # layout the final HGVS expression in curie format
        hgvs: str = f'{ref_chromosome}:g.{variation}'

        # return the expression to the caller
        return hgvs

    #############
    # get_expression_direction - gets the polarity of slope to get the direction of expression.
    #                            positive value increases expression, negative decreases
    #
    # param slope: str - the float value to determine direction of expression
    # return (str, str) - a label ID name
    #############
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

    #############
    # get_hyper_edge_id() - create a MD5 hash int of a hyper edge ID using the composite string:
    #                       <uberon tissue id>_<ensemble gene id>_<variant CAID id>
    #
    # param uberon: str - the uberon ID
    # param ensembl: str - the ensembl ID
    # param variant: str - the variant ID
    # return hyper_edge_id: int - the hyper edge ID composite
    #############
    @staticmethod
    def get_hyper_edge_id(uberon: str, ensembl: str, variant: str) -> int:
        # check the input parameters
        if uberon is None or ensembl is None or variant is None:
            hyper_edge_id = 0
        else:
            # create a composite hyper edge id. the components of the composite are: (in this order):
            # <uberon tissue id>_<ensemble gene id>_<variant CAID id>
            composite_id = str.encode(f'{uberon}_{ensembl}_{variant}')

            # now MD5 hash the encoded string and turn it into an int
            hyper_edge_id = int(hashlib.md5(composite_id).hexdigest()[:8], 16)

        # return to the caller
        return hyper_edge_id

    #############
    # get_sequence_variant_obj - Creates a SequenceVariant object out of the variant id data field.
    #                            this also converts the variant_id to a HGVS expression along the way
    #
    #       The variant id layout is:
    #           chr, position, ref, alt, hg version
    #           ex: 1_762345_A_G_b37 becomes NC_000001.10:g.762345A>G
    #
    # param gtex_variant_id: str - the variant ID from the raw GTEx data
    # return SequenceVariant: namedtuple - elements extracted from the GTEx variant id
    #############
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

    #############
    # write_new_association - Writes an association edge with properties into the graph DB
    #
    # param writer: BufferedWriter - writer for the edge information
    # param source_node: KNode - source node data object
    # param associated_node : KNode - associated node data object
    # param predicate : LabeledID - object with ID and label for the edge
    # param hyper_edge_id : int - composite hyper edge ID
    # param properties : list = None - edge data properties
    # param force_create : bool = False) - forces the creation of the node edge even if exists
    # return KEdge - node to node edge created
    #######
    def write_new_association(self, writer: WriterDelegator, source_node: KNode, associated_node: KNode, predicate: LabeledID, hyper_edge_id: int, properties: list = None, force_create: bool = False) -> KEdge:
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

    #############
    # prepopulate_variant_synonymization_cache - populate the variant synonymization cache by walking through the variant list
    #                                            and batch synonymize any that need it
    #
    # param data_directory: str - the directory of the data file
    # param file_names: list - the name of the data file
    # returns: object, pass if it is none, otherwise an exception object
    #############
    #######
    def prepopulate_variant_synonymization_cache(self, data_directory: str, file_name: str) -> object:
        logger.info("Starting variant synonymization cache pre-population")

        # init the return value
        ret_val = None

        # create an array to bucket the uncached variants
        uncached_variants = []

        # init a line counter
        line_counter = 0

        try:
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

                        # look up the variant by the HGVS expression
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

        logger.info(f'Variant synonymization cache pre-population complete. Processed: {line_counter} variants.')

        # return to the caller
        return ret_val

    #######
    # process_variant_synonymization_cache - processes an array of un-cached variants by HGVS expression.
    #
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
                        caid_labeled_id = syn

                        # remove the synonym from the list
                        synonyms.remove(caid_labeled_id)

                        # set the new synonymization id
                        redis_pipe.set(f'synonymize({caid_labeled_id.identifier})', pickle.dumps(synonyms))

                        # add it back to the list with the new info
                        synonyms.add(caid_labeled_id)

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
