import os
from greent.graph_components import KNode
from greent.servicecontext import ServiceContext
from greent.services.ontology import GenericOntology

'''
def test_generic_ontology ():
    obo_path = os.path.join (os.path.dirname(__file__), '..', 'mondo.obo')
    ont = GenericOntology (ServiceContext.create_context (), obo_path)

    # Label
    label = ont.label ('MONDO:0009757')
    assert label == 'Niemann-Pick disease, type C1', 'incorrect label'
    print (f"(label): {label}")
    
    # is_a
    is_a = ont.is_a('MONDO:0012771', 'DOID:630')
    print (f"(is_a): {is_a}")
    assert is_a, 'Is a test failed.'
    
    # xrefs
    xrefs = ont.xrefs ('MONDO:0012771')
    assert len(xrefs) > 0, "xref search failed"
    print (f"(xrefs): {xrefs}")

    # synonyms
    synonyms = ont.synonyms ('MONDO:0012771')
    print (f"(synonyms) {synonyms}")
    assert any(map(lambda v : 'Asthma-Related Traits' in v.desc, synonyms)), "unexpected synonym list"

    # search
    result = ont.search ('huntington', is_regex=True)
    print (f"(search): {result}")
    assert len(result) > 0, "search failed"
'''
