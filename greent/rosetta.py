import unittest
import networkx as nx
import networkx.algorithms as nxa
from networkx.exception import NetworkXNoPath
import operator

class Rosetta:
    def __init__(self, config):
        from greent.core import GreenT
        self.core = GreenT ()
        self.g = nx.DiGraph ()
        self.vocab = config["@vocab"]
        for k in self.vocab:
            self.g.add_node (self.vocab[k])
        self.curie = config["@curie"]
        transitions = config["@transitions"]
        for L in transitions:
            for R in transitions[L]:
                print ("  +edge: {0} {1} {2}".format (L, R, transitions[L][R]))
                self.g.add_edge (L, R, data=transitions[L][R])
                self.g.add_edge (self.vocab[L], self.vocab[R], data=transitions[L][R])
    def guess_type (self, thing, source):
        if not source and ':' in thing:
            curie = thing.upper ().split (':')[0]
            if curie in self.curie:
                source = self.vocab[self.curie[curie]]
        return source
    def translate (self, thing, source, target):
        source = self.guess_type (thing, source)
#        print ("source: {}".format (source))
        transitions = self.get_transitions (source, target)
        print ("transition> {0}".format (transitions))
        last = thing
        for transition in transitions:
            try:
                op = operator.attrgetter(transition)(self.core) 
                print ("  -> calling {0}({1})".format (transition, last))
                this = op (last)
                print ("  -> calling {0}({1}) = {2}".format (transition, last, this))
                last = this
            except:
                pass
        return last
    def get_transitions (self, source, dest):
        print ("get-transitions: {0} {1}".format (source, dest))
        transitions = []
        try:
            paths = nxa.all_shortest_paths (self.g, source=source, target=dest)
            for path in paths:
                print ("  path: {0}".format (path))
                steps = list(zip(path, path[1:]))
                print ("  steps: {}".format (steps))
                for step in steps:
                    print ("    step: {}".format (step))
                    edges = self.g.edges (step, data=True)
                    for e in edges:
                        if step[1] == e[1]:
                            print ("      trans: {0} {1}".format (e, e[2]['data']['op']))
                            transition = e[2]['data']['op']
                            transitions.append (transition)
        except NetworkXNoPath:
            pass
        except KeyError:
            pass
        print ("-------------------> {}".format (transitions))
        return transitions

if __name__ == "__main__":
    translator = Rosetta (config = {
        "@curie" : {
            "DOID" : "doid"
        },
        "@vocab" : {
            "c2b2r_drug_name"     : "http://chem2bio2rdf.org/drugbank/resource/Generic_Name",
            "c2b2r_gene"          : "http://chem2bio2rdf.org/uniprot/resource/gene",
            "c2b2r_pathway"       : "http://chem2bio2rdf.org/kegg/resource/kegg_pathway",
            "doid"                : "http://identifiers.org/doid",
            "hetio_anatomy"       : "http://identifier.org/hetio/anatomy",
            "hetio_cell"          : "http://identifier.org/hetio/cellcomponent",
            "hgnc_id"             : "http://identifier.org/hgnc/gene/id",
            "mesh"                : "http://identifiers.org/mesh",
            "mesh_disease_id"     : "http://identifiers.org/mesh/disease/id",
            "mesh_disease_name"   : "http://identifiers.org/mesh/disease/name",
            "mesh_drug_name"      : "http://identifiers.org/mesh/drug/name",
            "pharos_disease_id"   : "http://pharos.nih.gov/identifier/disease/id",
            "pharos_disease_name" : "http://pharos.nih.gov/identifier/disease/name",
            "root_kind"           : "http://identifiers.org/doi",
        },
        "@transitions" : {
            "mesh_disease_name" : {
                "mesh_drug_name"      : { "op" : "chemotext.disease_name_to_drug_name" }
            },
            "doid"              : {
                "mesh_disease_id"     : { "op" : "disease_ontology.doid_to_mesh"   },
                "c2b2r_gene"          : { "op" : "pharos.disease_to_target"        },
                "pharos_disease_id"   : { "op" : "disease_ontology.doid_to_pharos" }
            },
            "c2b2r_drug_name"   : {
                "c2b2r_gene"          : { "op" : "chembio.drug_name_to_gene_symbol" }            
            },
            "c2b2r_gene"        : {
                "c2b2r_pathway"       : { "op" : "chembio.gene_symbol_to_pathway" },
                "pharos_disease_name" : { "op" : "pharos.target_to_disease" },
                "hetio_anatomy"       : { "op" : "hetio.gene_to_anatomy" },
                "hetio_cell"          : { "op" : "hetio.gene_to_cell" }
            },
            "pharos_disease_id" : {
                "hgnc_id"             : { "op" : "pharos.target_to_hgnc" }
            },
            "mesh"              : {
                "root_kind"           : { "op" : "oxo.mesh_to_other" }
            }
        }
    })


    test = {
        "c2b2r_gene" : "pharos_disease_name",
        "c2b2r_gene" : "hetio_cell",
        "mesh"       : "root_kind",
        "doid"       : "hgnc_id"
    }
#    for t in test:
#        print ("transitions: {}".format (translator.get_transitions (t, test[t])))
        
    things = [
        "DOID:0060728",
        "DOID:0050777"
    ]
    for t in things:
        translator.translate (t, None, translator.vocab["hgnc_id"])
