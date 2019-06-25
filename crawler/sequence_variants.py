from greent.util import LoggingUtil
from crawler.crawl_util import get_variant_list, prepopulate_variant_annotation_cache
from greent.graph_components import KNode
from greent import node_types

import logging
import pickle
import os

logger = LoggingUtil.init_logging("robokop-interfaces.crawler.sequence_variants", level=logging.DEBUG, logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

def load_sequence_variants(rosetta, force_reload=False):
    all_variants = set()
    if force_reload or not rosetta.core.gwascatalog.is_precached():
        all_variants = rosetta.core.gwascatalog.prepopulate_cache()
    else:
        logger.info('Already loaded gwascatalog into cache.')

    # could do the same thing for GTEX variants here 
    # all_variants.update(rosseta.core.gtex.get_the_variants())

    # then we batch process them together, as well as any other sources we want
    # do_something(all_variants)

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
# batch loads the MyVariant and Ensembl data
################
def load_MyVariant_and_Ensembl(rosetta: object, limit: int=None) -> object:
    # init the return value
    ret_val = None

    try:
        cache = rosetta.cache
        ensembl = rosetta.core.ensembl
        myvariant = rosetta.core.myvariant
        synonymizer = rosetta.synonymizer

        # get the list of variants
        var_list = get_all_variant_ids(rosetta, limit)

        # create an array to handle the ones not already in cache that need to be processed
        uncached_variant_annotation_nodes = []
        redis_counter = 0

        # get a handle to the redis pipe
        with cache.redis.pipeline() as redis_pipe:
            # for each variant
            for var in var_list:
                # check to see if we have all the data elements we need. element [0] is the ID, element [1] is the synonym list
                if len(var) == 2:
                    # create a variant node
                    variant_node = KNode(var[0], name=var[0], type=node_types.SEQUENCE_VARIANT)

                    # did we get a CA ID
                    if var[0].find("CAID") == 0:
                        # get the synonym data from the graph DB call
                        syn_set = set(var[1])

                        # add the synonyms to the node
                        variant_node.add_synonyms(syn_set)
                    # else let the regular way of synonymization do the work
                    else:
                        # synonymize to get all the meta data
                        synonymizer.synonymize(variant_node)

                if variant_node.id.find("CAID") == -1:
                    logger.info(f'Variant node {variant_node.id} does not have a CAID after synonymization.')

                # check if myvariant key exists in cache, otherwise add it to buffer for batch processing
                if cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})') is None:
                    uncached_variant_annotation_nodes.append(variant_node)

                # if there is enough in the variant annotation batch process them and empty the array
                if len(uncached_variant_annotation_nodes) == 1000:
                    prepopulate_variant_annotation_cache(cache, myvariant, uncached_variant_annotation_nodes)
                    uncached_variant_annotation_nodes = []

                # ensembl cant handle batches, and for now NEEDS to be pre-cached individually here
                nearby_cache_key = f'ensembl.sequence_variant_to_gene({variant_node.id})'

                # grab the nearby genes
                cached_nearby_genes = cache.get(nearby_cache_key)

                # were there any nearby genes not cached
                if cached_nearby_genes is None:
                    # get the data for the nearby genes
                    nearby_genes = ensembl.sequence_variant_to_gene(variant_node)

                    # add the key and data to the list to execute
                    redis_pipe.set(nearby_cache_key, pickle.dumps(nearby_genes))

                    # increment the counter
                    redis_counter += 1

                # do we have enough to process
                if redis_counter > 500:
                    # execute the redis load
                    redis_pipe.execute()

                    # reset the counter
                    redis_counter = 0

            # if there are remainder ensemble entries left to process
            if redis_counter > 0:
                redis_pipe.execute()

            # if there are remainder variant node entries left to process
            if uncached_variant_annotation_nodes:
                prepopulate_variant_annotation_cache(cache, myvariant, uncached_variant_annotation_nodes)

    except Exception as e:
        logger.error(f'Exception caught. Exception: {e}')
        ret_val = e

    # return to the caller
    return ret_val

# simple tester
# if __name__ == '__main__':
#     from greent.rosetta import Rosetta
#
#     # create a new builder object
#     data = load_MyVariant_and_Ensembl(Rosetta(), 100)
