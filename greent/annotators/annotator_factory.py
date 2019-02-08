from greent import node_types
from greent.annotators.gene_annotator import GeneAnnotator
from greent.annotators.chemical_annotator import ChemicalAnnotator
from greent.annotators.disease_annotator import DiseaseAnnotator
import logging

logger = logging.getLogger(name= __name__)
annotator_class_list = {
    node_types.GENE : GeneAnnotator,
    node_types.CHEMICAL_SUBSTANCE: ChemicalAnnotator,
    node_types.DISEASE: DiseaseAnnotator
}
annotator_instances = {}


def make_annotator(node, rosetta):
    """
    Factory of annotators. Maintains instances so data can be cached. 
    """
    if node.type not in annotator_instances:
        annotator_class = annotator_class_list.get(node.type)
        if annotator_class :
            annotator_instances[node.type] = annotator_class(rosetta)
        else :
            annotator_instances[node.type] =  None
    return annotator_instances[node.type]

def annotate_shortcut(node, rosetta):
    """
    Shortcut to calling the annotator, basically does making the annotator
    using the factory and calling it on the node. Returns none if no annotator
    was found.
    """
    annotator = make_annotator(node, rosetta)
    if annotator != None:
        return annotator.annotate(node)
    return None