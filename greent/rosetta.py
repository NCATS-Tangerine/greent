import logging
import unittest
import traceback
import networkx as nx
import networkx.algorithms as nxa
import operator
from networkx.exception import NetworkXNoPath
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging (__file__, logging.DEBUG)

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
                logger.debug ("  +edge: {0} {1} {2}".format (L, R, transitions[L][R]))
                self.g.add_edge (L, R, data=transitions[L][R])
                self.g.add_edge (self.vocab[L], self.vocab[R], data=transitions[L][R])
    def guess_type (self, thing, source):
        if thing and not source and ':' in thing:
            curie = thing.upper ().split (':')[0]
            if curie in self.curie:
                source = self.vocab[self.curie[curie]]
        return source
    def get_transitions (self, source, dest):
        logger.debug ("get-transitions: {0} {1}".format (source, dest))
        transitions = []
        try:
            paths = nxa.all_shortest_paths (self.g, source=source, target=dest)
            for path in paths:
                logger.debug ("  path: {0}".format (path))
                steps = list(zip(path, path[1:]))
                logger.debug ("  steps: {}".format (steps))
                for step in steps:
                    logger.debug ("    step: {}".format (step))
                    edges = self.g.edges (step, data=True)
                    for e in edges:
                        if step[1] == e[1]:
                            logger.debug ("      trans: {0} {1}".format (e, e[2]['data']['op']))
                            transition = e[2]['data']['op']
                            transitions.append (transition)
        except NetworkXNoPath:
            pass
        except KeyError:
            pass
        return transitions
    def translate (self, thing, source, target):
        if not thing:
            return None
        source = self.guess_type (thing, source)
        transitions = self.get_transitions (source, target)
        if len(transitions) > 0:
            print ("transition> {0}".format (transitions))
        last = thing
        for transition in transitions:
            try:
                op = operator.attrgetter(transition)(self.core) 
                this = op (last)
                if this:
                    print ("  -> calling {0}({1}) => {2}".format (transition, last, this[:min(1,len(this))]))
                last = this
            except:
                traceback.print_exc ()
        return last

if __name__ == "__main__":
    translator = Rosetta (config = {
        "@curie" : {
            "DOID" : "doid",
            "MESH" : "mesh"
        },
        "@vocab" : {
            "c2b2r_drug_name"     : "http://chem2bio2rdf.org/drugbank/resource/Generic_Name",
            "c2b2r_drug_id"       : "http://chem2bio2rdf.org/drugbank/resource/drugbank_drug",
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
            "mesh_disease_id"   : {
                "c2b2r_drug_id"       : { "op" : "chembio.get_drugs_by_condition" }
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
        "DOID:0050777",
        "DOID:2841"
    ]
    for t in things:
#        hgnc = translator.translate (t, None, translator.vocab["hgnc_id"])
        m    = translator.translate (t, None, translator.vocab["mesh_disease_id"])
        print (m)

        d    = translator.translate (m, translator.vocab["mesh_disease_id"], translator.vocab["c2b2r_drug_id"]) 
        print (d)
        '''
        g    = translator.translate (t, None, translator.vocab["c2b2r_gene"])
        print ("gene: {}".format (g))
        p    = translator.translate (g, "c2b2r_gene", translator.vocab["hgnc_id"])
        c    = translator.translate (g, "c2b2r_gene", translator.vocab["hetio_cell"])
        a    = translator.translate (g, "c2b2r_gene", translator.vocab["hetio_anatomy"])
        '''
