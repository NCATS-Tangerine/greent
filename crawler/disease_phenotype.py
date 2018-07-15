from crawler.crawl_util import glom, dump_cache

def load_diseases_and_phenotypes(rosetta):
    mondo_sets = build_sets(rosetta.core.mondo)
    hpo_sets = build_sets(rosetta.core.hpo)
    dicts = {}
    glom(dicts,mondo_sets)
    glom(dicts,hpo_sets)
    dump_cache(dicts,rosetta)

def build_sets(o):
    sets = []
    mids = o.get_ids()
    for mid in mids:
        dbx = set([x['id'] for x in o.get_xrefs(mid)])
        dbx.add(mid)
        sets.append(dbx)
    return sets

