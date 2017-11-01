import os
import json
import operator
import sys
import traceback

# pyswip
from pyswip import Prolog
from pyswip import registerForeign
from pyswip import Functor
from pyswip import call
from pyswip import newModule
from pyswip import Variable
from pyswip import Query
from pyswip.easy import getList, registerForeign

# problog
from problog.program import PrologFile
from problog.program import PrologString
from problog.formula import LogicFormula
from problog import get_evaluatable
from problog.logic import Term
from problog.logic import Var
from problog.logic import Constant
from problog.engine_stack import StackBasedEngine
from problog.engine import DefaultEngine

def test_problog():

    from problog.program import PrologString
    from problog import get_evaluatable

    text = open("translator_knowledgebase.prolog", 'r').read ()
    pl_model = PrologString(text)
    engine = DefaultEngine()
    db = engine.prepare(pl_model)

    cbr_gene = Term("cbr_gene")
    doid = Term("doid")
    hgnc_id = Term("hgnc_id")
    hetio_cell = Term("hetio_cell")
    translates = Term("translates")
    path_to = Term("path_to")
    query_term = path_to (cbr_gene, hetio_cell, None)

    doid_to_hgnc = path_to (doid, hgnc_id, None)
    res = engine.query (db, doid_to_hgnc)
    print ('%s? %s' % (doid_to_hgnc, res))

    reify = Term("reify")
    drug = Term("tS")
    res = engine.query(db, reify (drug, None, None))
    print ('%s? %s' % (reify, res))

    type_matrix   = Term ("type_matrix")
    disease       = Term ("tD")
    gene          = Term ("tG")
    res           = engine.query(db, type_matrix (disease, None, None, gene, None, None))
    print ('%s? %s' % (reify, res))
    parse_terms = lambda t : list(map(lambda s : Term(s), str(t).replace("[","").replace("]","").replace(",","").split (" ")))
    for r in res:
        L = parse_terms (r[2])
        R = parse_terms (r[5])
        print (" : %s %s " % (L, R))
        for anL in L:
            for anR in R:
                path = path_to (anL, anR, None)
                res = engine.query (db, path)
                if len(res) > 0:
                    print ('   -- %s? => %s' % (path, res))

    a = Term("A")
    b = Term("B")
    c = Term("concat_str")
    query = Term("query")
    q = c(a,b,None)
    res = engine.query(db, q)
    print ('%s? %s' % (q, res))


    a = Term ("a")
    instanceof = Term ("instance_of")
    q = instanceof (a, None)
    res = engine.query (db, q)
    print ("%s %s" % (q, res))
    
                    
    sys.exit (0)


test_problog ()




if __name__ == "__main__":
    p = Prolog ()
    p.consult 



