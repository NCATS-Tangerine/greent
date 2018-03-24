from greent.util import Text
from greent.util import LoggingUtil
import logging

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG, format='long')

def synonymize(node, gt):
    synonyms = get_synonyms(node,gt)
    # do we have any MeSH ids?   If not, we want to dig deeper and get some so that chemotext will work
    # As we modify our literature diving methods, we might not need this any more.
    try:
        double_check_for_mesh(node,synonyms,gt)
    except Exception as e:
        logger.error("Failure for getting MESH: {}".format(node.identifier))
        logger.error(e)
    # OK, we're not going to use them all, there's some BS PMIDs that come back...
    synonyms = {s for s in synonyms if not s.startswith('PMID')}
    #node.add_synonyms(synonyms)
    return synonyms

def get_synonyms(node, gt, distance=2):
    #OXO doesn't know about every kind of curie.  So let's see if it knows about our node identifier
    synonyms = get_synonyms_with_curie_check(node.identifier, gt, distance=distance)
    if len(synonyms) == 0:
        #OXO didn't know about it.  So we're going to call oxo with our (valid) synonyms
        known_synonyms = node.synonyms
        for s in known_synonyms:
            synonyms.update( get_synonyms_with_curie_check(s,gt, distance=distance) )
    return synonyms

def get_synonyms_with_curie_check( identifier,gt,distance=2):
    if gt.oxo.is_valid_curie_prefix( Text.get_curie(identifier)):
        synonyms = gt.oxo.get_synonymous_curies(identifier, distance=distance)
    else:
        synonyms = set()
    return synonyms

def double_check_for_mesh( node, new_synonyms, gt):
    all_synonyms = set()
    all_synonyms.update(node.synonyms)
    all_synonyms.update(new_synonyms)
    for s in all_synonyms:
        if Text.get_curie(s) == 'MESH':
            return
    #No Mesh Found
    meshs = set()
    for s in all_synonyms:
        meshs.update( get_particular_synonyms(s, 'MESH', gt, distance=3))
    node.add_synonyms(meshs)

def get_particular_synonyms( identifier, prefix, gt, distance ):
    newsyns = get_synonyms_with_curie_check(identifier, gt, distance=distance)
    return set( filter( lambda x: Text.get_curie(x) == prefix, newsyns))


