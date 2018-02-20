from greent import node_types
from greent.synonymizers import hgnc_synonymizer
from greent.synonymizers import oxo_synonymizer
from greent.synonymizers import substance_synonymizer

#The mapping from a node type to the synonymizing module
synonymizers = {
    node_types.GENE:hgnc_synonymizer,
    node_types.DISEASE:oxo_synonymizer,
    node_types.GENETIC_CONDITION:oxo_synonymizer,
    node_types.PHENOTYPE:oxo_synonymizer,
    node_types.DRUG:substance_synonymizer,
    #These ones don't do anything, but we should at least pick up MeSH identifiers where we can.
    node_types.PATHWAY:oxo_synonymizer,
    node_types.PROCESS:oxo_synonymizer,
    node_types.CELL:oxo_synonymizer,
    node_types.ANATOMY:oxo_synonymizer,
}

def synonymize(node, gt):
    """Given a node, determine its type and dispatch it to the correct synonymizer"""
    synonymizers[node.node_type].synonymize(node,gt)
