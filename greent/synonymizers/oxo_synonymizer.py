# FOR the moment, we're just using any synonyms that come back, but...
# TODO: Check against our list of prefixes in rosetta.yaml
def synonymize(node, gt):
    synonyms = gt.oxo.get_synonyms(node.identifier)
    # OK, we're not going to use them all, there's some BS PMIDs that come back...
    synonyms = {s for s in synonyms if not s.startswith('PMID')}
    node.add_synonyms(synonyms)
