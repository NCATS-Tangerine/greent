from greent.util import LoggingUtil
from crawler.crawl_util import get_variant_list, prepopulate_variant_annotation_cache
from greent.graph_components import KNode
from greent import node_types

import logging
import pickle

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

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
def get_all_variant_ids(rosetta: object) -> list:
    # call the crawler util function to get a simple list of variant ids
    var_list = get_variant_list(rosetta)

    # return to the caller
    return var_list

################
# batch loads the MyVariant and Ensembl data
################
def load_MyVariant_and_Ensemble(rosetta: object):
    cache = rosetta.cache
    ensembl = rosetta.core.ensembl
    myvariant = rosetta.core.myvariant
    synonymizer = rosetta.synonymizer

    # get the list of variants
    var_list = get_all_variant_ids(rosetta)

    # create an array to handle the ones not already in cache that need to be processed
    uncached_variant_annotation_nodes = []

    with cache.redis.pipeline() as redis_pipe:
        # for each variant
        for var in var_list:
            # create a variant node
            variant_node = KNode(var, name=var, type=node_types.SEQUENCE_VARIANT)

            # synonymize to get all the meta data
            synonymizer.synonymize(variant_node)

            # check if myvariant key exists in cache, otherwise add it to buffer for batch processing
            if cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})') is None:
                uncached_variant_annotation_nodes.append(variant_node)

            # if there is enough in the variant annotation batch process them and empty the array
            if len(uncached_variant_annotation_nodes) == 1000:
                prepopulate_variant_annotation_cache(cache, uncached_variant_annotation_nodes)
                uncached_variant_annotation_nodes = []

            # ensembl cant handle batches, and for now NEEDS to be pre-cached individually here
            # (the properties on the nodes needed by ensembl wont be available to the runner)
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
            prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)

# simple tester
if __name__ == '__main__':
    from greent.rosetta import Rosetta
    # create a new builder object
    data = load_MyVariant_and_Ensemble(Rosetta())

