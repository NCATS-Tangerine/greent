from crawler.genes import load_genes, load_annotations_genes
from greent.rosetta import Rosetta
from greent import node_types
from crawler.chemicals import load_chemicals, load_annotations_chemicals
from crawler.program_runner import load_all
from crawler.disease_phenotype import load_diseases_and_phenotypes
from crawler.omni import create_omnicache,update_omnicache
from datetime import datetime as dt
import argparse

def poolrun(type1,type2,rosetta):
    start = dt.now()
    psize = 10
    load_all(type1,type2,rosetta,psize)
    end = dt.now()
    print(f'Poolsize: {psize}, time: {end-start}')

def load_synonyms(rosetta=None,refresh_chemicals=False):
    if rosetta is None:
        rosetta = Rosetta()
    load_genes(rosetta)
    load_chemicals(rosetta,refresh=refresh_chemicals)
    load_diseases_and_phenotypes(rosetta)

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
    (node_types.GENE, node_types.CHEMICAL_SUBSTANCE),
    (node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, node_types.CHEMICAL_SUBSTANCE),
    (node_types.PHENOTYPIC_FEATURE, node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY),
    (node_types.DISEASE, node_types.ANATOMICAL_ENTITY),
    (node_types.CHEMICAL_SUBSTANCE, node_types.GENE),
    (node_types.GENE, node_types.GENE_FAMILY),
    (node_types.CELLULAR_COMPONENT, node_types.CHEMICAL_SUBSTANCE),
    (node_types.CELLULAR_COMPONENT, node_types.ANATOMICAL_ENTITY),
    (node_types.CELLULAR_COMPONENT, node_types.DISEASE),
    (node_types.CELLULAR_COMPONENT, node_types.CELL)
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
    parser.add_argument('-o','--omnicache', help='Load omnicorp from postgres to redis', action='store_true')
    parser.add_argument('--source', help='type from which to build')
    parser.add_argument('--target', help='type to which to build')
    parser.add_argument('-A', '--annotate', help='Preform adding annotation data to cache', action='store_true')
    args = parser.parse_args()
    run(args)

