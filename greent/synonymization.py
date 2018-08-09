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
from builder.question import LabeledID


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

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG, format='medium')

class Synonymizer:

    def __init__(self, concepts, rosetta):
        self.rosetta = rosetta
        self.concepts = concepts
        
    def synonymize(self, node):
        """Given a node, determine its type and dispatch it to the correct synonymizer"""
        logger.debug('syn {} {}'.format(node.id, node.type))
        key = f"synonymize({node.id})"
        #check the cache. If it's not in there, try to generate it
        synonyms = self.rosetta.cache.get(key)
        if synonyms is not None:
            logger.debug (f"cache hit: {key}")
        else:
            logger.debug (f"exec op: {key}")
            if node.type in synonymizers:
                synonyms = synonymizers[node.type].synonymize(node, self.rosetta.core)
                self.rosetta.cache.set (key, synonyms)
            else:
                logger.warn (f"No synonymizer registered for concept: {node.type}")
        if synonyms is not None:
            logger.debug(f"Number of synonyms:{len(synonyms)}")
            #for s in synonyms:
            #    logger.debug(f"New syn: {s}")
            node.add_synonyms(synonyms)
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
                node.synonyms.remove(LabeledID(identifier=lid, label=None))
            if len(labels) > 1 and ('' in labels):
                node.synonyms.remove(LabeledID(identifier=lid, label=''))
        #Now find the bset one for an id
        type_curies = self.concepts.get(node.type).id_prefixes
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
                node.id = potential_identifiers[0].identifier
                #Only replace the label if we have a label.
                if potential_identifiers[0].label != '':
                    node.name = potential_identifiers[0].label
                break
        #Remove any synonyms with extraneous prefixes.  The point of this is not so much to remove
        # unknown prefixes, as to make sure that if we got e.g. a meddra, and we downcast it to a disease,
        # that we don't end up with HP's in the equivalent ids.
        bad_synonyms = set()
        for synonym in node.synonyms:
            if isinstance(synonym, LabeledID):
                prefix = Text.get_curie(synonym.identifier)
            else:
                prefix = Text.get_curie(synonym)
            if prefix not in type_curies:
                bad_synonyms.add(synonym)
        for bs in bad_synonyms:
            node.synonyms.remove(bs)
        if node.id.startswith('DOID'):
            logger.warn("We are ending up with a DOID here")
            logger.warn(node.id)
            logger.warn(node.synonyms)
            logger.warn(node.type)

