import pytest
from greent.graph_components import KNode,LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def myvariant(rosetta):
    myvariant = rosetta.core.myvariant
    return myvariant

@pytest.fixture()
def clingen(rosetta):
    clingen = rosetta.core.clingen
    return clingen

@pytest.fixture()
def gwascatalog(rosetta):
    gwascatalog = rosetta.core.gwascatalog
    return gwascatalog

@pytest.fixture()
def biolink(rosetta):
    biolink = rosetta.core.biolink
    return biolink

def test_sugen_file_to_graph(rosetta, myvariant, gwascatalog, biolink):

    results = parse_sugen_file('./sample_sugen.txt', .01, 'NC_0000', '11')
    assert len(results) > 0

    new_nodes = []
    relationships = []
    identifiers = [assoc["id"] for assoc in results]
    for identifier in identifiers:
        variant_node = KNode(identifier, type=node_types.SEQUENCE_VARIANT)
        rosetta.synonymizer.synonymize(variant_node)
        new_nodes.append(variant_node)
        relationships.extend(myvariant.sequence_variant_to_gene(variant_node))
        relationships.extend(gwascatalog.sequence_variant_to_phenotype(variant_node))
        relationships.extend(biolink.sequence_variant_get_phenotype(variant_node))

    assert len(new_nodes) > 0
    assert len(relationships) > 0

    predicates = [ relation.standard_predicate for relation,n in relationships ] 
    plabels = set( [p.label for p in predicates] )
    assert 'is_nearby_variant_of' in plabels

def parse_sugen_file(filename, p_value_cutoff, reference_genome, reference_version):

    try:
        results = []
        with open(filename) as f:
            headers = next(f).split()
            if ('PVALUE' in headers):
                pval_index = headers.index('PVALUE')
            else:
                print(f'Invalid file format: {filename}')
                return results
            for line in f:
                data = line.split()
                chromosome = data[0]
                if len(chromosome) == 1:
                    chromosome = f'0{chromosome}'
                position = data[1]
                ref_allele = data[3]
                alt_allele = data[4]
                p_value = data[pval_index]
                if (float(p_value) <= p_value_cutoff):
                    if ((ref_allele and (len(ref_allele) is 1)) and (alt_allele and (len(alt_allele) is 1))):
                        hgvs = f'{reference_genome}{chromosome}.{reference_version}:g.{position}{ref_allele}>{alt_allele}'
                        results.append({"id":f'HGVS:{hgvs}', "pvalue":p_value})
                    else:
                        print(f'Format of variant not recognized for hgvs conversion: {data}')

        return results

    except IOError:
        print(f'Could not open file: {filename}')
