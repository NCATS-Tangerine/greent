from greent.util import Text, LoggingUtil
from greent.graph_components import KNode
from greent import node_types
from greent.services.myvariant import MyVariant
from greent.export_delegator import WriterDelegator
from greent.cache import Cache
from builder.gtex_builder import GTExBuilder
from builder.question import LabeledID
from crawler.crawl_util import query_the_graph

import logging
import pickle
import os

logger = LoggingUtil.init_logging("robokop-interfaces.crawler.sequence_variants", level=logging.INFO, logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

default_gtex_file = 'signif_variant_gene_pairs.csv'

def load_gwas_knowledge(rosetta: object, limit: int = None):
    synonymizer = rosetta.synonymizer
    gwas_catalog_dict = rosetta.core.gwascatalog.prepopulate_cache()
    counter = 0
    with WriterDelegator(rosetta) as writer:
        for variant_node, relationships in gwas_catalog_dict.items():
            if relationships:
                writer.write_node(variant_node)
                for (gwas_edge, phenotype_node) in relationships:
                    # these phenotypes are probably already in the DB, but not necessarily
                    writer.write_node(phenotype_node)
                    writer.write_edge(gwas_edge)
            else:
                logger.error(f'GWASCatalog node {variant_node.id} had no phenotypes associated with it.')

            counter += 1
            if limit and counter == limit:
                break

def load_gtex_knowledge(rosetta: object, gtex_filenames=[]):
    # create a new builder object
    gtb = GTExBuilder(rosetta)

    # directory with GTEx data to process
    gtex_data_directory = f'{os.environ["ROBOKOP_HOME"]}/gtex_knowledge/'

    # assign the name of the GTEx data file
    associated_file_names = gtex_filenames if gtex_filenames else [default_gtex_file]

    # load up all the GTEx data
    rv = gtb.load(gtex_data_directory, associated_file_names)

    # check the return, output error if found
    if rv is not None:
        logger.error(rv)

def get_all_variant_ids_from_graph(rosetta: object) -> list:
    all_lids = []
    custom_query = 'match (s:sequence_variant) return distinct s.id'
    var_list = query_the_graph(rosetta, custom_query)
    for variant in var_list:
        all_lids.append(LabeledID(variant[0], variant[0]))
    return all_lids

def get_all_variants_and_synonymns(rosetta: object) -> list:
    custom_query = 'match (s:sequence_variant) return distinct s.id, s.equivalent_identifiers'
    return query_the_graph(rosetta, custom_query)

def get_gwas_knowledge_variants_from_graph(rosetta: object) -> list:
    custom_query = 'match (s:sequence_variant)-[x]-(d:disease_or_phenotypic_feature) where "gwascatalog.sequence_variant_to_disease_or_phenotypic_feature" in x.edge_source return distinct s.id'
    gwas_lids = []
    var_list = query_the_graph(rosetta, custom_query)
    for variant in var_list:
        gwas_lids.append(LabeledID(variant[0], variant[0]))
    return gwas_lids

def get_variants_without_genes_from_graph(rosetta: object) -> list:
    custom_query = 'match (s:sequence_variant) where not (s)--(:gene) return distinct s.id'
    variants_without_genes = []
    var_list = query_the_graph(rosetta, custom_query)
    for variant in var_list:
        variants_without_genes.append(LabeledID(variant[0], variant[0]))
    return variants_without_genes

def get_variants_and_synonyms_without_genes_from_graph(rosetta: object) -> list:
    custom_query = 'match (s:sequence_variant) where not (s)--(:gene) return distinct s.id, s.equivalent_identifiers'
    variants_without_genes = []
    return query_the_graph(rosetta, custom_query)

################
# batch precache any sequence variant data
################
def precache_variant_batch_data(rosetta: object, force_all: bool=False) -> object:
    # init the return value
    ret_val = None

    try:
        cache = rosetta.cache
        myvariant = rosetta.core.myvariant

        # get the list of variants
        if force_all:
            var_list = get_all_variants_and_synonymns(rosetta)
        else:
            # grab only variants with no existing gene relationships 
            var_list = get_variants_and_synonyms_without_genes_from_graph(rosetta)

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
