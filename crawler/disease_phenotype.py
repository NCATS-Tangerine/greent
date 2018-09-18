from crawler.crawl_util import glom, dump_cache
from builder.question import LabeledID
from greent.util import Text

def load_diseases_and_phenotypes(rosetta):
    mondo_sets = build_exact_sets(rosetta.core.mondo)
    hpo_sets = build_sets(rosetta.core.hpo)
    meddra_umls = read_meddra()
    dicts = {}
    glom(dicts,mondo_sets)
    glom(dicts,hpo_sets)
    glom(dicts,meddra_umls)
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


def build_sets(o):
    sets = []
    mids = o.get_ids()
    for mid in mids:
        #FWIW, ICD codes tend to be mapped to multiple MONDO identifiers, leading to mass confusion. So we
        #just excise them here.  It's possible that we'll want to revisit this decision in the future.  If so,
        #then we probably will want to set a 'glommable' and 'not glommable' set.
        dbx = set([Text.upper_curie(x['id']) for x in o.get_xrefs(mid) if not x['id'].startswith('ICD')])
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


