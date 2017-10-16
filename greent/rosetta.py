import logging
import unittest
import json
import logging
import os
import traceback
import networkx as nx
import networkx.algorithms as nxa
import operator
from networkx.exception import NetworkXNoPath
from greent.util import LoggingUtil
from pprint import pformat

logger = LoggingUtil.init_logging (__file__, logging.DEBUG)

class Translation (object):
    def __init__(self, obj, type_a=None, type_b=None, description="", then=None):
#        print ("Translation(obj:{0}, type_a: {1} type_b: {2})".format (obj, type_a, type_b))
        self.obj = obj
        self.type_a = type_a
        self.type_b = type_b
        self.desc = description
        self.then = []
        self.response = None
    def __repr__(self):
        return "Translation(obj: {0} type_a: {1} type_b: {2} desc: {3} then: {4} response: {5})".format (
            self.obj, self.type_a, self.type_b, self.desc, "", #self.then,
            pformat (self.response [: min(len(self.response), 2)] if self.response else ""))

default_router_config = {
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
        "genetic_condition"   : "http://identifiers.org/mondo/gentic_condition",
        "hetio_anatomy"       : "http://identifier.org/hetio/anatomy",
        "hetio_cell"          : "http://identifier.org/hetio/cellcomponent",
        "hgnc_id"             : "http://identifier.org/hgnc/gene/id",
        "mesh"                : "http://identifiers.org/mesh",
        "mesh_disease_id"     : "http://identifiers.org/mesh/disease/id",
        "mesh_disease_name"   : "http://identifiers.org/mesh/disease/name",
        "mesh_drug_name"      : "http://identifiers.org/mesh/drug/name",
        "pharos_disease_id"   : "http://pharos.nih.gov/identifier/disease/id",
        "pharos_disease_name" : "http://pharos.nih.gov/identifier/disease/name",
        "root_kind"           : "http://identifiers.org/doi"
    },
    "@transitions" : {
        "mesh_disease_name" : {
            "mesh_drug_name"      : { "op" : "chemotext.disease_name_to_drug_name" }
        },
        "mesh_disease_id"   : {
            "c2b2r_drug_id"       : { "op" : "chembio.get_drugs_by_condition_graph" }
        },
        "doid"              : {
            "mesh_disease_id"     : { "op" : "disease_ontology.doid_to_mesh"   },
#            "c2b2r_gene"          : { "op" : "pharos.disease_get_gene"        },
            "pharos_disease_id"   : { "op" : "disease_ontology.doid_to_pharos" },
            "hgnc_id"             : { "op" : "pharos.disease_get_gene"         }
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
        "hgnc_id"           : {
            "genetic_condition"   : { "op" : "biolink.gene_get_genetic_condition" }
        },
        "pharos_disease_id" : {
            "hgnc_id"             : { "op" : "pharos.target_to_hgnc" }
        },
        "mesh"              : {
            "root_kind"           : { "op" : "oxo.mesh_to_other" }
        }
    }
}

class Rosetta:
    def __init__(self, greentConf, config=default_router_config):
        from greent.core import GreenT
        self.core = GreenT (config=greentConf)
        self.g = nx.DiGraph ()
        self.vocab = config["@vocab"]
        for k in self.vocab:
            self.g.add_node (self.vocab[k])
        self.curie = config["@curie"]
        # import cmungall's uber context.
        with open(os.path.join (os.path.dirname (__file__), "jsonld", "uber_context.jsonld"), "r") as stream:
            uber = json.loads (stream.read ())
            context = uber['@context']
            for k in context:
                self.curie[k] = context[k]
                self.vocab[k] = context[k]
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
                source = self.curie[curie]
        if not source.startswith ("http://"):
            source = self.vocab[source] if source in self.vocab else None
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
#    def map_translations (self, translations):
#        results = []
#        for t in translations:
#            results.append (self.translate (thing=t.obj, source=t.type_a, target=t.type_b))
#        return results
    def translate (self, thing, source, target):
        if not thing:
            return None
        source = self.guess_type (thing, source)
        target = self.guess_type (None, target)
        transitions = self.get_transitions (source, target)
#        if len(transitions) > 0:
#            print ("transition> {0}".format (transitions))
        last = thing
        if len(transitions) == 0:
            logger.debug ("   [transitions:{2}] {0}->{1}".format (source, target, len(transitions)))
        else:
            logger.debug ("              [transitions:{3}] {0}->{1} {2}".format (source, target, transitions, len(transitions)))
        for transition in transitions:
            try:
                op = operator.attrgetter(transition)(self.core) 
                this = op (last)
                logger.debug ("              invoke: {0}({1}) => {2}".format (transition, last, this))
                last = this
            except:
                traceback.print_exc ()
#        print ("--------------> {}".format (last))
        return last if len(transitions) > 0 else None

if __name__ == "__main__":
    translator = Rosetta ()
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
    quiet = [ "connectionpool", "requests" ]
    for q in quiet:
        logging.getLogger(q).setLevel(logging.WARNING)
    for t in things:
#        hgnc = translator.translate (t, None, translator.vocab["hgnc_id"])
#        m    = translator.translate (t, None, translator.vocab["mesh_disease_id"])
        #print (m)

        logging.getLogger("chembio").setLevel (logging.DEBUG)
        m = 'MESH:D001249'
        d    = translator.translate (m, translator.vocab["mesh_disease_id"], translator.vocab["c2b2r_drug_id"])

        print (d)
        '''
        g    = translator.translate (t, None, translator.vocab["c2b2r_gene"])
        print ("gene: {}".format (g))
        p    = translator.translate (g, "c2b2r_gene", translator.vocab["hgnc_id"])
        c    = translator.translate (g, "c2b2r_gene", translator.vocab["hetio_cell"])
        a    = translator.translate (g, "c2b2r_gene", translator.vocab["hetio_anatomy"])
        '''
