
def load_diseases_and_phenotypes(rosetta):
    mondo_sets = build_sets(rosetta.core.mondo)
    hpo_sets = build_sets(rosetta.core.hpo)
    dicts = {}
    addto(dicts,mondo_sets)
    addto(dicts,hpo_sets)

def build_sets(o):
    sets = []
    mids = o.get_ids()
    for mid in mids:
        dbx = set([x['id'] for x in o.get_xrefs(mid)])
        dbx.add(mid)
        sets.append(dbx)

def addto(d,sets):
    for s in sets:
        for identifier in s:
            print ()
