import json
import os
import sys
from collections import defaultdict
from pprint import pformat
from pprint import pprint
from greent.client import GraphQL
from collections import namedtuple
from flask_testing import LiveServerTestCase
from greent.util import LoggingUtil
import networkx as nx
import networkx.algorithms as nxa

logger = LoggingUtil.init_logging (__file__)

class Vocab(object):
    root_kind         = 'http://identifiers.org/doi'
    
    # MESH
    mesh              = 'http://identifiers.org/mesh'
    mesh_disease_name = 'http://identifiers.org/mesh/disease/name'
    mesh_drug_name    = 'http://identifiers.org/mesh/drug/name'
    mesh_disease_id   = 'http://identifiers.org/mesh/disease/id'
    
    # Disease
    doid_curie          = "doid"
    doid                = "http://identifiers.org/doid"
    pharos_disease_name = "http://pharos.nih.gov/identifier/disease/name"

    # DRUG
    c2b2r_drug_name   = "http://chem2bio2rdf.org/drugbank/resource/Generic_Name"
    
    # TARGET
    c2b2r_gene        = "http://chem2bio2rdf.org/uniprot/resource/gene"
    
    # PATHWAY
    c2b2r_pathway     = "http://chem2bio2rdf.org/kegg/resource/kegg_pathway"

    # CELL
    hetio_cell        = "http://identifier.org/hetio/cellcomponent"

    # ANATOMY
    hetio_anatomy     = "http://identifier.org/hetio/anatomy"
    
    # Semantic equivalence

    def __init__(self):
        self.equivalence = defaultdict(lambda: [])
        self.equivalence[self.doid_curie] = [ self.doid ]
        
        # https://github.com/prefixcommons/biocontext/blob/master/registry/uber_context.jsonld
        uber_context_path = os.path.join(os.path.dirname(__file__), 'jsonld', 'uber_context.jsonld')
        with open (uber_context_path, 'r') as stream:
            self.equivalence = json.loads (stream.read ())["@context"]
            self.equivalence["MESH"] = self.equivalence ["MESH.2013"]

class NoTranslation (Exception):
    def __init__(self, message=None):
        super(NoTranslation, self).__init__(message)
        
class Translation (object):
    def __init__(self, obj, type_a=None, type_b=None, description="", then=None):
        print ("type_a: {0} type_b: {1}".format (type_a, type_b))
        self.obj = obj
        self.type_a = type_a
        self.type_b = type_b
        self.desc = description
        self.then = []
        self.response = None
    def __repr__(self):
        return "obj: {0} type_a: {1} type_b: {2} desc: {3} then: {4} response: {5}".format (
            self.obj, self.type_a, self.type_b, self.desc, "", #self.then,
            pformat (self.response [: min(len(self.response), 2)] if self.response else ""))

class TranslationAST(object):
    def __init__(self):
        self.stack = []
        self.from_type = defaultdict (lambda:[])
        self.to_type = defaultdict (lambda:[])
    def __repr__(self):
        text = []
        for t in self.stack:
            for r in t.response:
                text.append (r)
        return ", ".join (text)
    
class Mox(object):
    def __init__(self, thing, obj_type):
        self.thing = thing
        self.obj_type = obj_type

class Translator(object):

    def __init__(self, core):
        self.core = core
        self.vocab = Vocab ()
        
        # Domain translation
        self.translator_router = defaultdict (lambda: defaultdict (lambda: NoTranslation ()))
        self.translator_router[Vocab.mesh_disease_name] = {
            Vocab.mesh_drug_name : lambda disease: self.core.chemotext.disease_name_to_drug_name (disease)
        }
        self.translator_router[Vocab.doid][Vocab.mesh_disease_id]             = lambda doid:    self.core.disease_ontology.doid_to_mesh (doid.upper())
        self.translator_router[Vocab.c2b2r_drug_name][Vocab.c2b2r_gene]       = lambda drug:    self.core.chembio.drug_name_to_gene_symbol (drug)
        self.translator_router[Vocab.c2b2r_gene][Vocab.c2b2r_pathway]         = lambda gene:    self.core.chembio.gene_symbol_to_pathway (gene)
        self.translator_router[Vocab.c2b2r_gene][Vocab.pharos_disease_name]   = lambda gene:    self.core.pharos.target_to_disease (gene)
        self.translator_router[Vocab.doid][Vocab.c2b2r_gene]                  = lambda gene:    self.core.pharos.disease_to_target (gene)
        self.translator_router[Vocab.mesh][Vocab.root_kind]                   = lambda mesh_id: self.core.oxo.mesh_to_other (mesh_id)
        self.translator_router[Vocab.c2b2r_gene][Vocab.hetio_anatomy]         = lambda gene:    self.core.hetio.gene_to_anatomy (gene)
        self.translator_router[Vocab.c2b2r_gene][Vocab.hetio_cell]            = lambda gene:    self.core.hetio.gene_to_cell (gene)
        
    def resolve_id (self, an_id, domain):
        if not an_id in domain:
            candidate = an_id
            an_id = None
            if candidate in self.vocab.equivalence:
                for alternative in self.vocab.equivalence[candidate]:
                    if alternative in domain:
                        # Postpone the problem of synonymy
                        an_id = alternative
                        logger.debug ("Selected alternative id {0} for input {1}".format (an_id, candidate))
                        break
                    # Also, if all candidates turn out not to be in the domain, we could recurse to try synonyms for them
        return an_id

    def get_translator_op (self, type_a, type_b):
        logger.debug ("Translator request: {0} {1}".format (type_a, type_b))
        operator = None
        resolved_a = self.resolve_id (type_a, self.translator_router)
        resolved_b = self.resolve_id (type_b, self.translator_router[type_a])
        logger.debug ("resolved_a: {0} resolved_b: {1}".format (resolved_a, resolved_b))
        if resolved_a and resolved_b:
            operator = self.translator_router[resolved_a][resolved_b]
            print ("----------> operator: {0}".format (operator))
        return operator
    
    def translate (self, translation):
        result = None
        operator = self.get_translator_op (translation.type_a, translation.type_b)
        if operator != None and not isinstance (operator, NoTranslation):
            logger.info ("Translating: {0}".format (translation))
            translation.response = operator (translation.obj)
        return translation.response
    
    def translate0 (self, thing, type_a, type_b):
        logger.info ("Translator request thing: {0} {1} {2}".format (thing, type_a, type_b))
        result = None
        resolved_a = self.resolve_id (type_a, self.translator_router)
        resolved_b = self.resolve_id (type_b, self.translator_router[type_a])
        logger.info ("resolved_a: {0} resolved_b: {1}".format (resolved_a, resolved_b))
        if resolved_a and resolved_b:
            logger.info ("Translating {0} of type {1} to {2}".format (thing, type_a, type_b)) 
            result = self.translator_router[resolved_a][resolved_b] (thing)
            if isinstance (result, NoTranslation):
                raise NoTranslation ("No translation implemented from domain {0} to domain {1}".format (type_a, type_b))
        return result

    # Think:

    mox_resolve = {
        "drug"    : lambda x : Mox( x, Vocab.c2b2r_drug_name ),
        "gene"    : lambda x : Mox( x, Vocab.c2b2r_gene ),
        "pathway" : lambda x : Mox( x, Vocab.c2b2r_pathway ),
        "cell"    : lambda x : Mox( x, Vocab.hetio_cell ),
        "anatomy" : lambda x : Mox( x, Vocab.hetio_anatomy ),
        "disease" : lambda x : Mox( x, Vocab.doid ), #, Vocab.pharos_disease_name ),
        None      : lambda x : Mox( None, None )
    }
    
    def translate_chain (self, request, limit_breadth=2):
        '''
        Execute a chain of translations.

        :param request: A translation request of a term.
        :param limit_breadth: A limit on how many instances of results from one request to process in the subsequent request.
        :type request: dict
        :type limit_breadth: int
        :return: return Returns a list of translation objects with results.
        :rtype: Translation

        :Example:
        
        response = g.translator.translate_chain (request={
            "iri"     : "mox://drug/gene/pathway/cell/anatomy/disease",
            "drug"    : "Aspirin",
            "disease" : "Asthma"
        })
        '''
        elements = request['iri'].split ("mox://")[1].split ("/")
        ast = self.parse (elements, request)
        self.execute (ast, limit_breadth)
        return ast
    
    def execute (self, ast, limit_breadth):
        result = []
        for index, translation in enumerate (ast.stack):
            if translation.obj != None:
                output = self.translate (translation)
            else:
                antecedents = ast.to_type [translation.type_a]
                for pindex, previous in enumerate (antecedents):
                    if pindex > limit_breadth:
                        break
                    if previous.response == None:
                        continue
                    for item in previous.response:
                        # print ("-------> {0} {1}".format (type(item), item))
                        fragment = Translation (obj=item,
                                                type_a=translation.type_a,
                                                type_b=translation.type_b)
                        self.translate (fragment)
                        print ("%%%%>> {}".format (fragment))
                
    def execute_stack0 (self, stack, limit_breadth):
        for index, t in enumerate (stack):
            if t.obj == None and index > 0:
                last = stack [index - 1]
                logger.debug ("Last: {}".format (last))
                t.response = []
                if last.response:
                    for index, each in enumerate (last.response):
                        if limit_breadth < index:
                            break
                        t.response.append (self.translate (thing=each, type_a=last.type_b, type_b=t.type_b))
                        logger.info ("Translator response: {0}".format (pformat (t.response)))
                else:
                    logger.info ("last response is None for {}".format (last))
            else:
                t.response = self.translate (thing=t.obj, type_a=t.type_a, type_b=t.type_b)
                logger.info ("Translator response: {0}".format (pformat (t.response)))

    def get_mox (self, element, request):
        return self.mox_resolve [element](request[element] if element in request else None)
    
    def parse (self, elements, request):
        ast = TranslationAST ()
        for index1, e in enumerate (elements):
            mox = self.get_mox (e, request)
            root = None
            for index2, eprime in enumerate (elements):
                moxprime = self.get_mox (eprime, request)
                translation = Translation (obj=mox.thing, type_a=mox.obj_type, type_b=moxprime.obj_type)
                ast.from_type[mox.obj_type].append (translation)
                ast.to_type[moxprime.obj_type].append (translation)
                if root == None:
                    root = translation
                else:
                    root.then.append (translation)
                logger.debug(" --translation> {0}".format (translation))
                ast.stack.append (translation)
        return ast

    def parse_to_ast0 (self, elements, request):
        stack = []
        for index, e in enumerate (elements):
            mox = self.mox_resolve[e](request[e] if e in request else None)
            next_e = elements[index+1] if index < len(elements) - 1 else None
            print ("ne: {}".format (next_e))
            if next_e != None:
                value = request[next_e] if next_e in request else None
                next_mox = self.mox_resolve [next_e](value)
                logger.info (" next mox: {}".format (next_mox))
            else:
                next_mox = self.mox_resolve[None](None)
            translation = Translation (obj=mox.thing, type_a=mox.obj_type, type_b=next_mox.obj_type)
            logger.debug("Built translation: {0}".format (translation))
            stack.append (translation)
        return stack
