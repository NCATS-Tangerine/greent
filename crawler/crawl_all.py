from crawler.genes import load_genes, load_annotations_genes
from crawler.sequence_variants import load_gtex_knowledge, load_gwas_knowledge, get_gwas_knowledge_variants_from_graph, precache_variant_batch_data
from greent.rosetta import Rosetta
from greent import node_types
from crawler.chemicals import load_chemicals, load_annotations_chemicals
from crawler.program_runner import load_all
from crawler.disease_phenotype import load_diseases_and_phenotypes
from crawler.omni import create_omnicache,update_omnicache
from datetime import datetime as dt
import argparse

def poolrun(type1,type2,rosetta,identifier_list=None):
    start = dt.now()
    psize = 10
    # identifier_list defaults to none because
    # load_all will find it's own list in that case
    load_all(type1,type2,rosetta,psize,identifier_list=identifier_list)
    end = dt.now()
    print(f'Poolsize: {psize}, time: {end-start}')

def load_synonyms(rosetta=None,refresh_chemicals=False):
    if rosetta is None:
        rosetta = Rosetta()
    load_genes(rosetta)
    load_chemicals(rosetta,refresh=refresh_chemicals)
    load_diseases_and_phenotypes(rosetta)

def load_genetic_variants(rosetta=None):
    if rosetta is None:
        rosetta = Rosetta()

    # load starting set of variants into the graph
    print('loading the GWAS Catalog...')
    load_gwas_knowledge(rosetta)
    # or test with a smaller number of variants
    #load_gwas_knowledge(rosetta, limit=25)
    print('GWAS Catalog loading complete...')

    # load default gtex knowledge
    print('loading GTEx Data...')
    load_gtex_knowledge(rosetta)
    # or from a specific list of files
    #load_gtex_knowledge(rosetta, ['test_signif_Adipose_Subcutaneous_100.csv'])
    print('finished loading GTEx Data...')

def crawl_genetic_variants(rosetta=None):
    # run variant->variant crawl on gwas knowledge variants only
    gwas_labled_ids = get_gwas_knowledge_variants_from_graph(rosetta)
    poolrun(node_types.SEQUENCE_VARIANT, node_types.SEQUENCE_VARIANT, rosetta, identifier_list=gwas_labled_ids)

    # grab variants from the graph and do any batch processing / precaching we need to do
    print('batch cache preloading for genetic variants...')
    precache_variant_batch_data(rosetta, force_all=True)
    print('finished batch cache preloading for genetic variants...')

    # run variant->gene on every variant in the graph
    poolrun(node_types.SEQUENCE_VARIANT, node_types.GENE, rosetta)

crawls = [
    (node_types.DISEASE, node_types.PHENOTYPIC_FEATURE),
    (node_types.GENETIC_CONDITION, node_types.PHENOTYPIC_FEATURE),
    (node_types.PHENOTYPIC_FEATURE, node_types.DISEASE),
    (node_types.DISEASE, node_types.GENE),
    (node_types.GENE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY),
    (node_types.ANATOMICAL_ENTITY, node_types.PHENOTYPIC_FEATURE),
    (node_types.ANATOMICAL_ENTITY, node_types.CELL),
    (node_types.CELL, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY),
    (node_types.DISEASE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY),
    (node_types.DISEASE, node_types.CHEMICAL_SUBSTANCE),
    (node_types.CHEMICAL_SUBSTANCE, node_types.DISEASE),
    (node_types.CHEMICAL_SUBSTANCE, node_types.PHENOTYPIC_FEATURE),
    (node_types.CHEMICAL_SUBSTANCE, node_types.CHEMICAL_SUBSTANCE),
    (node_types.CHEMICAL_SUBSTANCE, node_types.FOOD),
    (node_types.GENE, node_types.CHEMICAL_SUBSTANCE),
    (node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, node_types.CHEMICAL_SUBSTANCE),
    (node_types.PHENOTYPIC_FEATURE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY),
    (node_types.DISEASE, node_types.ANATOMICAL_ENTITY),
    (node_types.CHEMICAL_SUBSTANCE, node_types.GENE),
    (node_types.GENE, node_types.GENE_FAMILY),
    (node_types.CELLULAR_COMPONENT, node_types.CHEMICAL_SUBSTANCE),
    (node_types.CELLULAR_COMPONENT, node_types.ANATOMICAL_ENTITY),
    (node_types.CELLULAR_COMPONENT, node_types.DISEASE),
    (node_types.CELLULAR_COMPONENT, node_types.CELL),
    (node_types.GENE_FAMILY, node_types.CELLULAR_COMPONENT),
    (node_types.GENE_FAMILY, node_types.GENE),
    (node_types.GENE_FAMILY, node_types.PATHWAY),
    (node_types.GENE_FAMILY, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY),
    (node_types.GENE_FAMILY, node_types.GENE_FAMILY)
]

def crawl_all(rosetta):
    load_synonyms(rosetta)
    create_omnicache(rosetta)
    for (source,target) in crawls:
        poolrun(source,target,rosetta)

def load_annotations(rosetta):
    load_annotations_chemicals(rosetta)
    load_annotations_genes(rosetta)

def run(args):
    rosetta = Rosetta()
    if args.all:
        print('all')
        crawl_all(rosetta)
    elif args.synonyms:
        print('synonyms')
        load_synonyms(rosetta)
    elif args.load_genetics:
        print('load genetic variation')
        load_genetic_variants(rosetta)
    elif args.crawl_genetics:
        print('crawl genetic variation')
        crawl_genetic_variants(rosetta)
    elif args.omnicache:
        print('omnicache')
        create_omnicache(rosetta)
    elif args.annotate:
        print('annotate')
        load_annotations(rosetta)
    else:
        print(f'crawl from {args.source} to {args.target}')
        poolrun(args.source, args.target,rosetta)

if __name__=='__main__':
    #run crawl_all.py -h to see the list of allowed crawls
    helpstring = 'Allowed crawls (source)->(target):\n'+'\n'.join([f'  {c[0]}->{c[1]}' for c in crawls])
    parser = argparse.ArgumentParser(description=helpstring,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-a','--all', help='Perform all crawls consecutively', action='store_true')
    parser.add_argument('-s','--synonyms', help='Build all synonyms (genes, chemicals, diseases, phenotypes)', action='store_true')
    parser.add_argument('-lg','--load_genetics', help='Load genetic variant knowledge', action='store_true')
    parser.add_argument('-cg','--crawl_genetics', help='Crawl additional genetic variant knowledge', action='store_true')
    parser.add_argument('-o','--omnicache', help='Load omnicorp from postgres to redis', action='store_true')
    parser.add_argument('--source', help='type from which to build')
    parser.add_argument('--target', help='type to which to build')
    parser.add_argument('-A', '--annotate', help='Preform adding annotation data to cache', action='store_true')
    args = parser.parse_args()
    run(args)

