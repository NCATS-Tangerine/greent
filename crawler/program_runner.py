from builder.question import LabeledID
from greent import node_types, config
from builder.buildmain import run
from multiprocessing import Pool
from functools import partial
from crawler.crawl_util import pull_via_ftp, get_variant_list
from json import loads
from greent.graph_components import KNode
from greent.util import Text
import requests

# There's a tradeoff here: do we want these things in the database or not.  One big problem
# is that they end up getting tangled up in a huge number of explosive graphs, and we almost
# never want them.  So let's not put them in, at least to start out...
conf = config.Config('greent.conf')

bad_idents = conf.get('bad_identifiers')
# ('MONDO:0021199', # Disease by anatomical system,
# 'MONDO:0000001', # Disease,
# 'MONDO:0021194', # Disease by subcellular system affected,
# 'MONDO:0021195', # Disease by cellular process disrupted,
# 'MONDO:0021198', # Rare Genetic Disease,
# 'MONDO:0003847', # Inherited Genetic Disease,
# 'MONDO:0003847', # Rare Disease,
# 'UBERON:0000468', # Multicellular Organism)
#               )

def get_label(curie, url='https://uberonto.renci.org/label/'):
    y = {'label': ''}
    try:        
        y.update(requests.get(f'{url}/{curie}').json())
        return y
    except Exception as e:
        print(e)
        return y

def get_identifiers(input_type,rosetta):
    lids = []
    if input_type == node_types.DISEASE:
        identifiers =  rosetta.core.mondo.get_ids()
        for ident in identifiers:
            if ident not in bad_idents:
                label = rosetta.core.mondo.get_label(ident)
                if label is not None and not label.startswith('obsolete'):
                    lids.append(LabeledID(ident,label))
    if input_type == node_types.PHENOTYPIC_FEATURE:
        identifiers = rosetta.core.hpo.get_ids()
        for ident in identifiers:
            if ident not in bad_idents:
                label = rosetta.core.hpo.get_label(ident)
                if label is not None and not label.startswith('obsolete'):
                    lids.append(LabeledID(ident,label))
    elif input_type == node_types.GENETIC_CONDITION:
        identifiers_disease = rosetta.core.mondo.get_ids()
        for ident in identifiers_disease:
            print(ident)
            if ident not in bad_idents:
                if rosetta.core.mondo.is_genetic_disease(KNode(ident,type=node_types.DISEASE)):
                    label = rosetta.core.mondo.get_label(ident)
                    if label is not None and not label.startswith('obsolete'):
                        print(ident,label,len(lids))
                        lids.append(LabeledID(ident,label))
    elif input_type == node_types.ANATOMICAL_ENTITY:
        identifiers = requests.get("https://uberonto.renci.org/descendants/UBERON:0001062").json()
        for ident in identifiers:
            if ident not in bad_idents:
                res = get_label(ident) #requests.get(f'https://uberonto.renci.org/label/{ident}').json()
                lids.append(LabeledID(ident,res['label']))
    elif input_type == node_types.CELL:
        identifiers = requests.get("https://uberonto.renci.org/descendants/CL:0000000").json()
        for ident in identifiers:
            if ident not in bad_idents:
                res = get_label(ident) #requests.get(f'https://uberonto.renci.org/label/{ident}/').json()
                lids.append(LabeledID(ident,res['label']))
    elif input_type == node_types.GENE:
        print("Pull genes")
        data = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/json', 'hgnc_complete_set.json')
        hgnc_json = loads( data.decode() )
        hgnc_genes = hgnc_json['response']['docs']
        for gene_dict in hgnc_genes:
            symbol = gene_dict['symbol']
            lids.append( LabeledID(identifier=gene_dict['hgnc_id'], label=symbol) )
        print("OK")
    elif input_type == node_types.CELLULAR_COMPONENT:
        print('Pulling cellular compnent descendants')
        identifiers = requests.get("https://uberonto.renci.org/descendants/GO:0005575").json()
        # for now trying with exclusive descendants of cellular component 
        for ident in identifiers:
            if ident not in bad_idents:
                res = get_label(ident) #requests.get(f'https://uberonto.renci.org/label/{ident}/').json()
                lids.append(LabeledID(ident,res['label']))

    elif input_type == node_types.CHEMICAL_SUBSTANCE:
        print('pull chem ids')
        identifiers = requests.get("https://uberonto.renci.org/descendants/CHEBI:23367").json()
        identifiers = [x for x in identifiers if 'CHEBI' in x]
        print('pull labels...')
        #This is the good way to do this, but it's soooooo slow
        #n = 0
        #for ident in identifiers:
        #    if n % 100 == 0:
        #        print(n,ident)
        #    n+=1
        #    res = requests.get(f'http://onto.renci.org/label/{ident}/').json()
        #    lids.append(LabeledID(ident,res['label']))
        #Instead:
        chebiobo = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/ontology','chebi_lite.obo' ).decode()
        lines = chebiobo.split('\n')
        chebi_labels = {}
        for line in lines:
            if line.startswith('[Term]'):
                tid = None
                label = None
            elif line.startswith('id:'):
                tid = line[3:].strip()
            elif line.startswith('name:'):
                label = line[5:].strip()
                chebi_labels[tid] = label
        # go for KEGG
        print('pull KEGG')
        content = requests.get('http://rest.kegg.jp/list/compound').content.decode('utf-8')
    
        for line in content.split('\n'):
            if line :
                identifier, label = line.split('\t')
                identifier = identifier.replace('cpd', 'KEGG.COMPOUND')
                identifier= identifier.replace('CPD', 'KEGG.COMPOUND')
                # maybe pick the first one for kegg,
                label = label.split(';')[0].strip(' ')
                lids.append(LabeledID(identifier, label))
        
        for ident in identifiers:
            try:
                lids.append(LabeledID(ident,chebi_labels[ident]))
            except KeyError:
                res = get_label(ident) #requests.get(f'https://uberonto.renci.org/label/{ident}/').json()
                lids.append(LabeledID(ident,res['label']))

    elif input_type == node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY:
        # pull Biological process decendants
        identifiers = requests.get('https://uberonto.renci.org/descendants/GO:0008150').json()
        # merge with molucular activity decendants
        identifiers = identifiers + requests.get('https://uberonto.renci.org/descendants/GO:0003674').json()

        for ident in identifiers:
            if ident not in bad_idents:
                p = get_label(ident) #requests.get(f'https://uberonto.renci.org/label/{ident}/')
                lids.append(LabeledID(ident, p['label']))
    elif input_type == node_types.GENE_FAMILY:
        gene_fam_data = rosetta.core.panther.gene_family_data
        for key in gene_fam_data:
            name = gene_fam_data[key]['family_name']
            name = f'{name} ({key})' if 'NOT NAMED' in name else name
            lids.append(LabeledID(f'PANTHER.FAMILY:{key}', name))
            sub_keys = [k for k in gene_fam_data[key].keys() if k !='family_name']
            for k in sub_keys:
                name = gene_fam_data[key][k]['sub_family_name']
                name = f'{name} ({key})' if 'NOT NAMED' in name else name
                lids.append(LabeledID(f'PANTHER.FAMILY:{key}:{k}',gene_fam_data[key][k]['sub_family_name'] ))        

    elif input_type == node_types.SEQUENCE_VARIANT:
        # grab every variant already in the graph
        #var_list = get_variant_list(rosetta, limit=30)
        var_list = get_variant_list(rosetta)
        for variant in var_list:
            lids.append(LabeledID(variant[0], Text.un_curie(variant[0])))
    else:
        print(f'Not configured for input type: {input_type}')

    return lids

def do_one(itype,otype,identifier):
    path = f'{itype},{otype}'
    print(path)
    if type(identifier) != type([]):
        print('Passing single identifier per program')
        run(path,identifier.label,identifier.identifier,None,None,None,'greent.conf')
    else:
        print('passing chunk of identifiers for a program')
        run(path,'','',None,None,None,'greent.conf', identifier_list = identifier)
     
def load_all(input_type, output_type,rosetta,poolsize):
    """Given an input type and an output type, run a bunch of workflows dumping results into neo4j and redis"""
    identifiers = get_identifiers(input_type,rosetta)
    print( f'Found {len(identifiers)} input {input_type}')
    partial_do_one = partial(do_one, input_type, output_type)
    pool = Pool(processes=poolsize)
    chunks = poolsize*2
    chunksize = int(len(identifiers)/chunks)
    print( f'Chunksize: {chunksize}')
    single_program_size = chunksize  if chunksize > 0 else 1 # nodes sent to a program
    identifier_chunks = [identifiers[i: i + single_program_size] for i in range(0, len(identifiers), single_program_size)]
    pool.map_async(partial_do_one, identifier_chunks)# chunksize=chunksize)
    pool.close()
    pool.join()
