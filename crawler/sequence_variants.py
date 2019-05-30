from greent.util import LoggingUtil
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

