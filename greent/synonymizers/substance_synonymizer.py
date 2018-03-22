from greent.util import Text
from greent.synonymizers import oxo_synonymizer
from greent.util import LoggingUtil
import logging

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

# Substances are a special case, because the landscape of identifiers is such a mess.
# Therefore, it's going to take a few different approaches in conjunction to get anywhere.
# If we start with a CTD:chemical, then we can get a MeSH from CTD, and go to OXO from there.
# If we have MeSH, UMLS, snomed, NCIT, Drugbank, CAS or CHEBI we can go to OXO to get the others
# From either CHEBI or DrugBank we can use UniChem to get CHEMBL.
# If we start with CHEMBL, on the other hand, then we can go to UniChem and then to OXO and CTD.
#
# This is all based on the idea that our two main methods of getting drug->gene are pharos and ctdbase.
# And these are also how we recognize names.  So if we get the name via pharos, then we have a CHEMBL id
# and convert UniChem,OXO,CTD. But if we got the name via CTD, then we go CTD,MeSh,OXO,UniChem
#
# If we add other drug name resolvers then things may change. Adding other functions that simply use compound ids
# should be ok, as long as these two paths resolve whatever the name of interest is.
def synonymize(node,gt):
    logger.debug("Synonymize: {}".format(node.identifier))
    curie = Text.get_curie(node.identifier)
    if curie == 'CHEMBL':
        synonymize_with_UniChem(node,gt)
        synonymize_with_OXO(node,gt)
        #synonymize_with_CTD(node,gt)
    else:
        synonymize_with_OXO(node,gt)
        synonymize_with_UniChem(node,gt)
        #synonymize_with_CTD(node,gt)


def synonymize_with_OXO(node,gt):
    logger.debug(" OXO: {}".format(node.identifier))
    oxo_synonymizer.synonymize(node,gt)
    logger.debug("  updated syns: {}".format( ','.join(list(node.synonyms))))

def synonymize_with_UniChem(node,gt):
    logger.debug(" UniChem: {}".format(node.identifier))
    all_synonyms = set()
    for synonym in node.synonyms:
        curie = Text.get_curie(synonym)
        if curie in ('CHEMBL', 'CHEBI', 'DRUGBANK'):
            new_synonyms = gt.unichem.get_synonyms( synonym )
            all_synonyms.update(new_synonyms)
    node.add_synonyms( all_synonyms )
    logger.debug("  updated syns: {}".format( ','.join(list(node.synonyms))))


