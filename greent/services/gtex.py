from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import LoggingUtil
from greent.graph_components import KEdge

from collections import namedtuple

import hashlib
import time

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
    # create static edge labels for variant/gtex and gene/gtex edges
    variant_gtex_label = LabeledID(identifier=f'GTEx:affects_expression_in', label=f'affects expression in')
    gene_gtex_label = LabeledID(identifier=f'gene_to_expression_site_association', label=f'gene to expression site association')

    ########
    # constructor
    ########
    def __init__(self, context, rosetta):
        super(GTEx, self).__init__("gtex", context)
        self.synonymizer = rosetta.synonymizer
        self.concept_model = rosetta.type_graph.concept_model

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
        self.synonymizer.synonymize(variant_node)


        # create a predicate label

        #

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
        self.synonymizer.synonymize(variant_node)

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
        self.synonymizer.synonymize(gene_node)

        return None

    ########
    # define the manual way to launch processing an input data file
    ########
    @staticmethod
    def create_gtex_graph(file_path, file_names):
        # check the inputs
        if file_path is None or file_names is None:
            logger.error('Error: Missing or invalid input arguments')

        # create a new builder object
        # gtb = GTExBuilder(Rosetta())

        # load the redis cache with GTEx data
        # gtb.prepopulate_gtex_catalog_cache()

        # call the GTEx builder to load the cache and graph database
        # gtb.create_gtex_graph(file_path, file_names, 'GTEx service')

        return None


#############
# Class: GTExUtils
# By: Phil Owen
# Date: 5/21/2019
# Desc: A class that has a number of shared static functions between the GTEx service and builder.
#############
class GTExUtils:
    # object to store the details for a variant
    SequenceVariant = namedtuple('sequencevariant', ['build', 'chrom', 'pos', 'ref', 'alt', 'hgvs', 'node'])
    SequenceVariant.__new__.__defaults__ = (None, None)

    # object to store GTEx details of the variant
    GTExVariant = namedtuple('gtexvariant', ['tissue_name', 'uberon', 'ensembl', 'pval_nominal', 'slope'])
    GTExVariant.__new__.__defaults__ = (None, None)

    # object to store data parsing objects
    DataParsingResults = namedtuple('data_parsing', ['SequenceVariant', 'GTExVariant'])
    DataParsingResults.__new__.__defaults__ = (None, None)

    #######
    # list of chromosome number for HGVS gene assembly version conversions (hg37, hg38)
    #######
    reference_chrom_labels = {
        'b37': {
            'p1': {
                1: 'NC_000001.10',  2: 'NC_000002.11', 3: 'NC_000003.11', 4: 'NC_000004.11', 5: 'NC_000005.9',
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
                18: 'NC_000018.10', 19: 'NC_000019.10',  20: 'NC_000020.11', 21: 'NC_000021.9', 22: 'NC_000022.11', 23: 'NC_000023.11',
                24: 'NC_000024.10'
            }
        }
    }

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
    # noinspection PyCallByClass
    @staticmethod
    def get_sequence_variant_obj(variant_id):
        try:
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
            ref_chromosome = GTExUtils.reference_chrom_labels[reference_genome][reference_patch][chromosome]
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

        # convert the reference genome to a project standard. danger, hack job ahead.
        if reference_genome == 'b37':
            reference_genome = 'HG19'
        else:
            reference_genome = 'HG38'

        # create the sequence_variant object
        seq_var = GTExUtils.SequenceVariant(reference_genome, chromosome, position, ref_allele, alt_allele, hgvs=hgvs, node=None)

        # return the expression to the caller
        return seq_var

    #######
    # write_new_association - Writes an association edge with properties into the graph DB
    #######
    @staticmethod
    def write_new_association(writer, source_node, associated_node, predicate, hyper_edge_id, concept_model, properties=None, force_create=False):
        # if the concept model is loaded standardize the predicate label
        if concept_model:
            standard_predicate = concept_model.standardize_relationship(predicate)
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
