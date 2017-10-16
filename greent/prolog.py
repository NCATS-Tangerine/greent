import os
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
from problog.engine_stack import StackBasedEngine
from problog.engine import DefaultEngine


def test_problog():
    model="translator_knowledgebase.prolog"
    program = PrologFile(model)
    formula = LogicFormula.create_from(program)
    #db = StackBasedEngine (program)
    
    get_evaluatable().create_from(program).evaluate()
    #q = PrologString ("query(path_to(doid, pharos_disease_id)).")
    q = PrologString ("path_to(doid, pharos_disease_id).")
    #get_evaluatable().create_from(q).evaluate ()

    engine = DefaultEngine()
    db = engine.prepare(program)
    query1 = Var('query(path_to(doid,X)).', None)   # query for 'heads(_)'
    results = engine.query(db=db, term=query1)
    print (results)

    query1 = Term('path_to(doid,Y).', 'Y')   # query for 'heads(_)'
    results = engine.query(db=db, term=query1)
    print (results)
    
    #print (db.query ("query(path_to(doid, pharos_disease_id)).", term=Term(None)))
    sys.exit (0)






class Reason:
    def __init__(self):
        self.prolog = Prolog ()
        self.execute (self.read_file (os.path.join (os.path.dirname (__file__), "translator_knowledgebase.prolog")))
        self.prolog.consult (os.path.join (os.path.dirname (__file__), "translator_rules.prolog"))
    def read_file(self, f):
        result = None
        with open(f,"r") as stream:
            result = stream.read ()
        return result
    def execute (self, code):
        for assertion in code.split ("\n"):
            if len(assertion) == 0:
                continue
            print (assertion)
            self.prolog.assertz (assertion)
    def query (self, query):
        solutions = []
        if len(query) > 0:
            print ("query: [{0}]".format (query))
            try:
                solutions += self.prolog.query (query)
                for solution in solutions:
                    for k in solution:
                        print ("   {0}: {1}".format (k, solution[k]))
            except:
                traceback.print_exc ()
        return solutions

class Sub:
    def x (self, arg):
        print ("sub x-----{0}".format (arg))        
class Trans:
    r = Reason ()
    s = Sub ()
    def a (self):
        print ("a----")
    def b(self):
        print ("b---")
    def execute (self, q):
        solutions = self.r.query (q)
        if solutions:
            for solution in solutions:
                if "C" in solution:
                    try:
                        op = operator.attrgetter(solution["C"])(self)
                        op ("hi")
                    except:
                        #traceback.print_exc ()
                        pass
class Notifier:
    def notify (self, args):
        print ("notify------------------ {}".format (args))
    notify.arity=1

if __name__ == "__main__":
    p = Prolog ()
    p.consult 

    query="""
    is_a(X,drug)
    is_a(tD,X), has_context(X,Y)
    is_a(tD,X), has_context(X,Types), findall(Q, (member(Y,Types), is_type(Y, Q)), Qs)
    is_a(tD,X), has_context(X,Types), findall(Q, (member(Y,Types), is_type(Y, Q)), Qs), findall(T, (member(S,Types), translates(S, D, T)), Ts)
    translates(cbr_drug_name, cbr_gene, C)
    path_to(tD,Y)
    transitions(X,Y,Ts)
    """
    
    notifier = Notifier ()
 #   registerForeign (notifier.notify)
    
    t = Trans ()
    for q in query.split ("\n"):
        t.execute (q.strip ())



