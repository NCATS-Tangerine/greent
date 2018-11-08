import pytest
from greent.graph_components import KNode,LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

def test_sugen_file_to_graph(rosetta):

    obh = ObesityHub(rosetta)
    results = obh.create_variant_graph('./sample_sugen.txt', .0000000000000000001, 'NC_0000', '11')
    assert results == 0

    results = obh.create_variant_graph('./sample_sugen.txt', .00001, 'NC_0000', '11')
    assert results == 6

class ObesityHub(object):

    def __init__(self, rosetta):
        self.rosetta = rosetta
        self.myvariant = rosetta.core.myvariant
        self.gwascatalog = rosetta.core.gwascatalog
        self.biolink = rosetta.core.biolink

    def create_variant_graph(self, filename, p_value_cutoff, reference_genome, reference_version):
        new_nodes = []
        relationships = []
        try:
            with open(filename) as f:
                headers = next(f).split()
                if ('PVALUE' in headers):
                    pval_index = headers.index('PVALUE')
                else:
                    print(f'Invalid file format: {filename}')
                    return results
                for line in f:
                    data = line.split()
                    if len(data) < pval_index:
                        continue
                    chromosome = data[0]
                    if len(chromosome) == 1:
                        chromosome = f'0{chromosome}'
                    position = data[1]
                    ref_allele = data[3]
                    alt_allele = data[4]
                    p_value = data[pval_index]
                    try:
                        if float(p_value) <= p_value_cutoff:
                            # we could use this if genome was a SequenceFileDB object:
                            #hgvs_name = pyhgvs.format_hgvs_name(chromosome, position, ref_allele, alt_allele, genome, transcript)
                            if (ref_allele and (len(ref_allele) is 1)) and (alt_allele and (len(alt_allele) is 1)):
                                hgvs = f'{reference_genome}{chromosome}.{reference_version}:g.{position}{ref_allele}>{alt_allele}'
                                variant_node = KNode(f'HGVS:{hgvs}', type=node_types.SEQUENCE_VARIANT)
                                self.rosetta.synonymizer.synonymize(variant_node)
                                new_nodes.append(variant_node)

                                relationships.extend(self.myvariant.sequence_variant_to_gene(variant_node))
                                relationships.extend(self.gwascatalog.sequence_variant_to_phenotype(variant_node))
                                relationships.extend(self.biolink.sequence_variant_get_phenotype(variant_node))
                            #else:
                                #print(f'Format of variant not recognized for hgvs conversion: {data}')
                    except ValueError:
                        continue
                        # should we log this? p value wasn't a float

        except IOError:
            print(f'Could not open file: {filename}')

        return len(new_nodes)
