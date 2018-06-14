import logging
from collections import defaultdict

from greent import node_types
from greent.graph_components import LabeledID
from greent.util import Text, LoggingUtil
from greent.synonymizers import cell_synonymizer
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
    node_types.FUNCTION:oxo_synonymizer,
    node_types.PROCESS_OR_FUNCTION:oxo_synonymizer,
    node_types.CELL:cell_synonymizer,
    node_types.ANATOMY:oxo_synonymizer,
}

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

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
                logger.debug (f"cache hit: {key}")
            else:
                logger.debug (f"exec op: {key}")
                synonyms = synonymizers[node.node_type].synonymize(node, self.rosetta.core)
                self.rosetta.cache.set (key, synonyms)
            node.add_synonyms(synonyms)
        else:
            logger.warn (f"No synonymizer registered for concept: {node.node_type}")
        self.normalize(node)

    def normalize(self,node):
        """Given a node, which will have many potential identifiers, choose the best identifier to be the node ID,
        where 'best' is defined by the order in which identifiers appear in the id prefix configurations within the concept model."""
        #If we have two synonyms with the same id, but one has no label, chuck it
        smap = defaultdict(list)
        for labeledid in node.synonyms:
            smap[labeledid.identifier].append(labeledid.label)
        for lid,labels in smap.items():
            if len(labels) > 1 and (None in labels):
                node.synonyms.remove(LabeledID(lid,None))
            if len(labels) > 1 and ('' in labels):
                node.synonyms.remove(LabeledID(lid,''))
        #Now find the bset one for an id
        type_curies = self.concepts.get(node.node_type).id_prefixes
        #Now start looking for the best curies
        synonyms_by_curie = defaultdict(list)
        for s in node.synonyms:
            c = Text.get_curie(s.identifier)
            synonyms_by_curie[c].append(s)
        for type_curie in type_curies:
            potential_identifiers = synonyms_by_curie[type_curie]
            if len(potential_identifiers) > 0:
                if len(potential_identifiers) > 1:
                    pis = [ f'{pi.identifier}({pi.label})' for pi in potential_identifiers]
                    ids_with_labels = list(filter(lambda x: x.label is not None, potential_identifiers ))
                    if len(ids_with_labels) > 0:
                        potential_identifiers = ids_with_labels
                    potential_identifiers.sort()
                node.identifier = potential_identifiers[0].identifier
                node.label = potential_identifiers[0].label
                break
        if node.identifier.startswith('DOID'):
            logger.warn("We are ending up with a DOID here")
            logger.warn(node.identifier)
            logger.warn(node.synonyms)
            logger.warn(node.node_type)

