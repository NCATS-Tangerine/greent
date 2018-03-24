import logging
from collections import defaultdict

from greent import node_types
from greent.util import Text, LoggingUtil
from greent.synonymizers import hgnc_synonymizer
from greent.synonymizers import oxo_synonymizer
from greent.synonymizers import substance_synonymizer
from greent.synonymizers import disease_synonymizer


#The mapping from a node type to the synonymizing module
synonymizers = {
    node_types.GENE:hgnc_synonymizer,
    node_types.DISEASE:disease_synonymizer,
    node_types.GENETIC_CONDITION:disease_synonymizer,
    node_types.PHENOTYPE:oxo_synonymizer,
    node_types.DRUG:substance_synonymizer,
    #These ones don't do anything, but we should at least pick up MeSH identifiers where we can.
    node_types.PATHWAY:oxo_synonymizer,
    node_types.PROCESS:oxo_synonymizer,
    node_types.CELL:oxo_synonymizer,
    node_types.ANATOMY:oxo_synonymizer,
}

logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)

class Synonymizer:

    def __init__(self, concepts, rosetta):
        self.rosetta = rosetta
        self.concepts = concepts
        
    def synonymize(self, node):
        """Given a node, determine its type and dispatch it to the correct synonymizer"""
        logger.debug('syn {} {}'.format(node.identifier,node.node_type))
        if node.node_type in synonymizers:
            key = f"synonymize({node.identifier})"
            synonyms = self.rosetta.cache.get (key)
            if synonyms is not None:
                logger.info (f"cache hit: {key}")
            else:
                logger.info (f"exec op: {key}")
                synonyms = synonymizers[node.node_type].synonymize(node, self.rosetta.core)
                self.rosetta.cache.set (key, synonyms)
            node.add_synonyms(synonyms)
        else:
            logger.warn (f"No synonymizer registered for concept: {node.node_type}")
        self.normalize(node)

    def normalize(self,node):
        """Given a node, which will have many potential identifiers, choose the best identifier to be the node ID,
        where 'best' is defined by the order in which identifiers appear in the id prefix configurations within the concept model."""
        type_curies = self.concepts.get(node.node_type).id_prefixes
        original_curie = Text.get_curie(node.identifier)
        if original_curie == type_curies[0]:
            #The identifier is already the best curie, so stop doing anything
            return
        #Now start looking for the best curies
        synonyms_by_curie = defaultdict(list)
        for s in node.synonyms:
            c = Text.get_curie(s)
            synonyms_by_curie[c].append(s)
        for type_curie in type_curies:
            potential_identifiers = synonyms_by_curie[type_curie]
            if len(potential_identifiers) > 0:
                if len(potential_identifiers) > 1:
                    logger.warn('More than one potential identifier for a node: {}'.format(','.join(potential_identifiers)))
                node.identifier = potential_identifiers[0]
                break

