from greent.util import Text
from greent.synonymizers import oxo_synonymizer

#2/20/2018, OXO doesn't yet understand MONDOs.
# So: if the identifier is a mondo identifier, pull down doids and whatever from mondo xrefs
#     then hit OXO in order of the best identifiers (?)
def synonymize(node,gt):
    curie = Text.get_curie(node.identifier)
    if curie == 'MONDO':
        synonymize_with_MONDO(node,gt)
    synonymize_with_OXO(node,gt)

def synonymize_with_MONDO(node,gt):
    syns = set(gt.mondo.mondo_get_doid( node.identifier ))
    syns.update( set(gt.mondo.mondo_get_umls( node.identifier )))
    syns.update( set(gt.mondo.mondo_get_efo( node.identifier )))
    node.add_synonyms(syns)

def synonymize_with_OXO(node,gt):
    oxo_synonymizer.synonymize(node,gt)

def testit():
    from greent.graph_components import KNode
    from greent import node_types
    node = KNode('MONDO:0005737',node_types.DISEASE)
    from greent.core import GreenT
    gt = GreenT()
    synonymize(node,gt)
    print("\n-----\n"+'\n'.join(list(node.synonyms)))

if __name__ == '__main__':
    testit()
