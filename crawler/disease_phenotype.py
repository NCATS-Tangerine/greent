from crawler.crawl_util import glom, dump_cache
from builder.question import LabeledID
from greent.util import Text

def write_sets(sets,fname):
    with open(fname,'w') as outf:
        for s in sets:
            outf.write(f'{s}\n')

def write_dicts(dicts,fname):
    with open(fname,'w') as outf:
        for k in dicts:
            outf.write(f'{k}\t{dicts[k]}\n')


def load_diseases_and_phenotypes(rosetta):
    mondo_sets = build_exact_sets(rosetta.core.mondo)
    write_sets(mondo_sets,'mondo_sets.txt')
    hpo_sets = build_sets(rosetta.core.hpo)
    write_sets(hpo_sets,'hpo_sets.txt')
    meddra_umls = read_meddra()
    write_sets(hpo_sets,'meddra_umls_sets.txt')
    dicts = {}
    glom(dicts,mondo_sets)
    write_dicts(dicts,'mondo_dicts.txt')
    glom(dicts,hpo_sets)
    write_dicts(dicts,'mondo_hpo_dicts.txt')
    glom(dicts,meddra_umls)
    write_dicts(dicts,'mondo_hpo_meddra_dicts.txt')
    with open('disease.txt','w') as outf:
        dump_cache(dicts,rosetta,outf)

def build_exact_sets(o):
    sets = []
    mids = o.get_ids()
    for mid in mids:
        #FWIW, ICD codes tend to be mapped to multiple MONDO identifiers, leading to mass confusion. So we
        #just excise them here.  It's possible that we'll want to revisit this decision in the future.  If so,
        #then we probably will want to set a 'glommable' and 'not glommable' set.
        dbx = [ Text.upper_curie(x) for x in o.get_exact_matches(mid) ]
        dbx = set( filter( lambda x: not x.startswith('ICD'), dbx ) )
        label = o.get_label(mid)
        mid = Text.upper_curie(mid)
        dbx.add(LabeledID(mid,label))
        sets.append(dbx)
    return sets


def norm(curie):
    if Text.get_curie(curie) == 'MSH':
        return f'MESH:{Text.un_curie(curie)}'
    if Text.get_curie(curie) == 'SNOMEDCT_US':
        return f'SNOMEDCT:{Text.un_curie(curie)}'
    return curie

def build_sets(o):
    sets = []
    mids = o.get_ids()
    for mid in mids:
        #FWIW, ICD codes tend to be mapped to multiple MONDO identifiers, leading to mass confusion. So we
        #just excise them here.  It's possible that we'll want to revisit this decision in the future.  If so,
        #then we probably will want to set a 'glommable' and 'not glommable' set.
        dbx = set([Text.upper_curie(x['id']) for x in o.get_xrefs(mid) if not x['id'].startswith('ICD')])
        dbx = set([norm(x) for x in dbx])
        label = o.get_label(mid)
        mid = Text.upper_curie(mid)
        dbx.add(LabeledID(mid,label))
        sets.append(dbx)
    return sets

#THIS is bad.
# We can't distribute MRCONSO.RRF, and dragging it out of UMLS is a manual process.
# It's possible we could rebuild using the services, but no doubt very slowly
def read_meddra():
    pairs = []
    with open('MRCONSO.RRF','r') as inf:
        for line in inf:
            x = line.strip().split('|')
            if x[1] != 'ENG':
                continue
            pairs.append( (f'UMLS:{x[0]}',f'MEDDRA:{x[13]}'))
    return pairs
