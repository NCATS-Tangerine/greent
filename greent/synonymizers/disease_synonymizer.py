from greent.util import Text
from greent.synonymizers import oxo_synonymizer
from greent.graph_components import  LabeledID
from builder.question import LabeledID

#2/20/2018, OXO doesn't yet understand MONDOs.
# So: if the identifier is a mondo identifier, pull down doids and whatever from mondo xrefs
#     then hit OXO in order of the best identifiers (?)
def synonymize(node,gt):
    curie = Text.get_curie(node.id)
    synonyms = set()
    if curie == 'MONDO':
        synonyms.update(synonymize_with_MONDO(node,gt))
        #You might think this is wrong,but it is right.  Even though the synonyms will get added to the node
        #outside, we are also going to add them here so that the OXO synonymizer will find them.
        node.synonyms.update(synonyms)
    synonyms.update(synonymize_with_OXO(node,gt))
    return synonyms

def synonymize_with_MONDO(node,gt):
    syns = set([ LabeledID(identifier=x, label="") for x in gt.mondo.mondo_get_doid( node.id )])
    syns.update( set( [ LabeledID(identifier=x, label="") for x in gt.mondo.mondo_get_umls( node.id )]) )
    syns.update( set( [ LabeledID(identifier=x, label="") for x in gt.mondo.mondo_get_efo( node.id )]))
    mondo_ids = {s.identifier for s in node.synonyms if s.identifier.split(':')[0].upper() == 'MONDO'}
    #node.add_synonyms(syns)
    return syns

def synonymize_with_OXO(node,gt):
    synonyms =  oxo_synonymizer.synonymize(node,gt)
    node.synonyms.update(synonyms)
    #Now, if we didn't start with a MONDO id, OXO is not going to give us one.
    #So let's get any doids we have and get a mondo from them
    mondos = {s.identifier for s in node.synonyms if s.identifier.split(':')[0].upper() == 'MONDO'}
    if len(mondos) == 0:
        doids = {s.identifier for s in node.synonyms if s.identifier.split(':')[0].upper() == 'DOID'}
        for doid in doids:
            moremondos = gt.mondo.get_mondo_id_and_label(doid)
            synonyms.update(moremondos)
    return synonyms


