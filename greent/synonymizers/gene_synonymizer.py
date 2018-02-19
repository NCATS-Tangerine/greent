from greent import node_types

def synonymize(node,gt):
    if not node.node_type == node_types.GENE:
        raise Exception("Incorrect node type")
    synonyms = gt.hgnc.get_synonyms(node.identifier)
    node.add_synonyms(synonyms)
