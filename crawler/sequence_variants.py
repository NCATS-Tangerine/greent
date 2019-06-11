from greent.util import LoggingUtil
from crawler.crawl_util import get_variant_list
import logging

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
# gets the list of sequence variant ids
################
def get_all_variant_ids(rosetta: list) -> list:
    # call the crawler util function to get a simple list of variant ids
    var_list = get_variant_list(rosetta)

    # TODO: work the list here?

    # return to the caller
    return var_list

# simple tester
# if __name__ == '__main__':
#     from greent.rosetta import Rosetta
#
#     # create a new builder object
#     data = get_all_variant_ids(Rosetta())

