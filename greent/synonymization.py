from greent import node_types
from greent.synonymizers import trivial_synonymizer
from greent.synonymizers import gene_synonymizer

#The mapping from a node type to the synonymizing module
synonymizers = {
    node_types.GENE:gene_synonymizer,
    node_types.DRUG:trivial_synonymizer,
    node_types.DISEASE:trivial_synonymizer,
    node_types.GENETIC_CONDITION:trivial_synonymizer,
    node_types.PHENOTYPE:trivial_synonymizer,
    #These ones don't do anything...
    node_types.PATHWAY:trivial_synonymizer,
    node_types.PROCESS:trivial_synonymizer,
    node_types.CELL:trivial_synonymizer,
    node_types.ANATOMY:trivial_synonymizer,
}

def synonymize(node):
    """Given a node, determine its type and dispatch it to the correct synonymizer"""
    synonymizers[node.node_type].synonymize(node)
