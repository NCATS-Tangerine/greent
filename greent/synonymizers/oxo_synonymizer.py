from greent.util import Text

# FOR the moment, we're just using any synonyms that come back, but...
# TODO: Check against our list of prefixes in rosetta.yaml
def synonymize(node, gt):
    #OXO doesn't know about every kind of curie.  So let's see if it knows about our node identifier
    synonyms = get_synonyms_with_curie_check(node.identifier, gt)
    if len(synonyms) == 0:
        #OXO didn't know about it.  So we're going to call oxo with our (valid) synonyms
        known_synonyms = node.synonyms
        for s in known_synonyms:
            synonyms.update( get_synonyms_with_curie_check(s,gt) )
    # OK, we're not going to use them all, there's some BS PMIDs that come back...
    synonyms = {s for s in synonyms if not s.startswith('PMID')}
    node.add_synonyms(synonyms)


def get_synonyms_with_curie_check( identifier,gt,distance=2):
    if gt.oxo.is_valid_curie_prefix( Text.get_curie(identifier)):
        synonyms = gt.oxo.get_synonymous_curies(identifier, distance=distance)
    else:
        synonyms = set()
    return synonyms
