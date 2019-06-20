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
from greent.synonymizers import sequence_variant_synonymizer
from builder.question import LabeledID


logger = LoggingUtil.init_logging(__name__, level=logging.INFO, format='medium')

class Synonymizer:

    def __init__(self, concepts, rosetta):
        self.rosetta = rosetta
        self.concepts = concepts
        self.fixed_synonymizers = {
            node_types.GENE:set([hgnc_synonymizer]),
            node_types.DISEASE:set([disease_synonymizer]),
            node_types.CHEMICAL_SUBSTANCE:set([substance_synonymizer]),
            node_types.CELL:set([cell_synonymizer]),
            node_types.SEQUENCE_VARIANT:set([sequence_variant_synonymizer])
        }
        self.create_synonymizers()

    def create_synonymizers(self):
        self.synonymizers = self.fixed_synonymizers
        top_set = [s.name for s in self.concepts.get_roots()]
        while len(top_set) > 0:
            next = top_set.pop()
            children = self.concepts.get_children(next)
            top_set.extend(children)
            if next in self.synonymizers:
                for child in children:
                    self.synonymizers[child] = self.synonymizers[next]
        roots = [s.name for s in self.concepts.get_roots()]
        for root in roots:
            self.recursive_set_synonymizer(root)


    def recursive_set_synonymizer(self,node_type):
        if node_type in self.synonymizers:
            return self.synonymizers[node_type]
        #not in there, make it the union of its children
        children = self.concepts.get_children(node_type)
        #but if there are no children, just use oxo
        if len(children) == 0:
            self.synonymizers[node_type] = set([oxo_synonymizer])
        else:
            synset = set()
            for c in children:
                synset.update( self.recursive_set_synonymizer(c) )
            self.synonymizers[node_type] = synset
        return self.synonymizers[node_type]

    def synonymize(self, node):
        """Given a node, determine its type and dispatch it to the correct synonymizer"""
        # logger.debug('syn {} {}'.format(node.id, node.type))
        key = f"synonymize({Text.upper_curie(node.id)})"
        #check the cache. If it's not in there, try to generate it
        try:
            synonyms = self.rosetta.cache.get(key)
        except Exception as e:
            # catch and log all errors
            # e.g. when the cached value cannot be reconciled with the LabeledID class
            logger.warning(e)
            synonyms = None
        if synonyms is not None:
            logger.debug (f"cache hit: {key}")
        else:
            logger.debug (f"exec op: {key}")
            if node.type in self.synonymizers:
                synonyms = set()
                for s in self.synonymizers[node.type]:
                    synonyms.update( s.synonymize(node, self.rosetta.core) )
                self.rosetta.cache.set (key, synonyms)
            else:
                logger.warn (f"No synonymizer registered for concept: {node.type}")
        if synonyms is not None:
            #logger.debug(f"Number of synonyms:{len(synonyms)}")
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
            num_left = len(labels)
            if num_left > 1 and None in labels:
                node.synonyms.remove(LabeledID(identifier=lid, label=None))
                num_left -= 1
            if num_left > 1 and '' in labels:
                node.synonyms.remove(LabeledID(identifier=lid, label=''))   
        #Now find the best one for an id
        type_curies = self.concepts.get(node.type).id_prefixes
        #Now start looking for the best curies
        synonyms_by_curie = defaultdict(list)
        for s in node.synonyms:
            c = Text.get_curie(s.identifier)
            synonyms_by_curie[c].append(s)
        for type_curie in type_curies:
            potential_identifiers = synonyms_by_curie[type_curie]
            #if the current identifier is in the list of possible identifiers, then let's keep it - don't switch!
            if node.id in [x.identifier for x in potential_identifiers]:
                break
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
        uc = [ tc.upper() for tc in type_curies ]
        for synonym in node.synonyms:
            if isinstance(synonym, LabeledID):
                prefix = Text.get_curie(synonym.identifier)
            else:
                prefix = Text.get_curie(synonym)
            if prefix == None or prefix.upper() not in uc:
                bad_synonyms.add(synonym)
        for bs in bad_synonyms:
            node.synonyms.remove(bs)
