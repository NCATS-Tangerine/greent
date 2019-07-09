from greent.util import LoggingUtil
from greent.graph_components import KNode
from greent import node_types
from greent.services.myvariant import MyVariant
from greent.export import BufferedWriter
from greent.cache import Cache
from crawler.crawl_util import get_variant_list
from builder.gtex_builder import GTExBuilder

import logging
import pickle
import os

logger = LoggingUtil.init_logging("robokop-interfaces.crawler.sequence_variants", level=logging.INFO, logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

def load_sequence_variants(rosetta):
    
    print('loading the GWAS Catalog...')
    load_gwas_catalog(rosetta)
    print('finished loading the GWAS Catalog...')

    print('loading GTEx Data...')
    load_gtex(rosetta)
    print('finished loading GTEx Data...')

def load_gwas_catalog(rosetta: object):
    synonymizer = rosetta.synonymizer
    gwas_catalog_dict = rosetta.core.gwascatalog.prepopulate_cache()
    print('writing the GWAS Catalog to the graph...')
    with BufferedWriter(rosetta) as writer:
        for variant_node, relationships in gwas_catalog_dict.items():
            # almost all of these will be precached but some rare cases such as two CAID per rsid still need to be synonymized
            synonymizer.synonymize(variant_node)
            writer.write_node(variant_node)
            for (gwas_edge, phenotype_node) in relationships:
                synonymizer.synonymize(phenotype_node)
                writer.write_node(phenotype_node)
                writer.write_edge(gwas_edge)

def load_gtex(rosetta: object):

    # create a new builder object
    gtb = GTExBuilder(rosetta)

    # directory with GTEx data to process
    gtex_data_directory = '/gtex_data/'

    # assign the name of the GTEx data file
    associated_file_names = ['signif_variant_gene_pairs.csv']

    # load up all the GTEx data
    rv = gtb.load(gtex_data_directory, associated_file_names)

    # check the return, output error if found
    if rv is not None:
        logger.error(rv)

################
# Gets the list of sequence variant ids
#
# param: Rosetta object
# return: a list of sequence variant IDs
################
def get_all_variant_ids(rosetta: object, limit: int=None) -> list:
    # call the crawler util function to get a simple list of variant ids
    var_list = get_variant_list(rosetta, limit)

    # return to the caller
    return var_list

################
# batch precache any sequence variant data
################
def precache_variant_batch_data(rosetta: object, limit: int=None) -> object:
    # init the return value
    ret_val = None

    try:
        cache = rosetta.cache
        myvariant = rosetta.core.myvariant

        # get the list of variants
        var_list = get_all_variant_ids(rosetta, limit)

        # create an array to handle the ones not already in cache that need to be processed
        uncached_variant_annotation_nodes = []

        # for each variant
        for var in var_list:
            # check to see if we have all the data elements we need. element [0] is the ID, element [1] is the synonym list
            if len(var) == 2:
                # create a variant node
                variant_node = KNode(var[0], name=var[0], type=node_types.SEQUENCE_VARIANT)

                # get the synonym data from the graph DB call
                syn_set = set(var[1])

                # add the synonyms to the node
                variant_node.add_synonyms(syn_set)

                # check if myvariant key exists in cache, otherwise add it to buffer for batch processing
                cache_results = cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})')

                if cache_results == None:
                    uncached_variant_annotation_nodes.append(variant_node)

                    # if there is enough in the variant annotation batch process them and empty the array
                    if len(uncached_variant_annotation_nodes) == 1000:
                        prepopulate_variant_annotation_cache(cache, myvariant, uncached_variant_annotation_nodes)
                        uncached_variant_annotation_nodes = []

        # if there are remainder variant node entries left to process
        if uncached_variant_annotation_nodes:
            prepopulate_variant_annotation_cache(cache, myvariant, uncached_variant_annotation_nodes)

    except Exception as e:
        logger.error(f'Exception caught. Exception: {e}')
        ret_val = e

    # return to the caller
    return ret_val


#######
# process_variant_annotation_cache - processes an array of un-cached variant nodes.
#######
def prepopulate_variant_annotation_cache(cache: Cache, myvariant: MyVariant, batch_of_nodes: list) -> bool:
    # init the return value, presume failure
    ret_val = False
    
    # get a batch of variants
    batch_annotations = myvariant.batch_sequence_variant_to_gene(batch_of_nodes)

    # do we have anything to process
    if len(batch_annotations) > 0:
        # open a connection to the redis cache DB
        with cache.redis.pipeline() as redis_pipe:
            # for each variant
            for seq_var_curie, annotations in batch_annotations.items():
                # assemble the redis key
                key = f'myvariant.sequence_variant_to_gene({seq_var_curie})'
    
                # add the key and data to the list to execute
                redis_pipe.set(key, pickle.dumps(annotations))
    
            # write the records out to the cache DB
            redis_pipe.execute()
            
            ret_val = True
            
    # return to the caller
    return ret_val

# simple tester
# if __name__ == '__main__':
#     from greent.rosetta import Rosetta
#
#     # create a new builder object
#     data = load_MyVariant_and_Ensembl(Rosetta(), 100)
