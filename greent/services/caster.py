from greent.service import Service
import operator
import functools
from greent.util import Text, LoggingUtil
from greent.graph_components import KNode, KEdge
from greent import node_types
import logging

logger = LoggingUtil.init_logging(__file__, level=logging.DEBUG)

class Caster(Service):

    def __init__(self, context, core):
        super(Caster, self).__init__("caster", context)
        self.core = core

    def output_filter(self, base_function, output_type, type_check, node):
        results = base_function(node)
        good_results = list(filter(lambda y: type_check(y[1]), results))
        for edge,node in good_results:
            node.node_type = output_type
        return good_results

    def upcast(self, base_function, output_type, node):
        results = base_function(node)
        for edge,node in results:
            node.node_type = output_type
        return results

    def output_filter(self, base_function, output_type, type_check, node):
        results = base_function(node)
        good_results = list(filter(lambda y: type_check(y[1]), results))
        for edge,node in good_results:
            node.node_type = output_type
        return good_results

    def input_filter(self, base_function, input_type, type_check, node):
        if type_check is not None:
            if not type_check(node):
                return []
        else:
            #We don't have a way to actually filter, so we're going to stuff whatever we have into base_function and
            #hope for the best
            pass
        return base_function(node)

    def unwrap(self,ftext):
        """We know that there will only be nested parens in the first argument"""
        l = ftext.index('(')
        fname = ftext[:l]
        argstring = ftext[l+1:-1]
        if ')' in argstring:
            r = argstring.rindex(')')
            arg0 = argstring[:r+1]
            args = [arg0] + argstring[r+1].split(',')
        else:
            args = argstring.split(',')
        return fname,args

    def create_function(self,functiontext):
        logger.debug(functiontext)
        if '(' in functiontext:
            fname, args = self.unwrap(functiontext)
            if fname == 'output_filter':
                newargs = [ self.create_function(args[0]), args[1], self.create_function(args[2])]
                return functools.partial( self.output_filter, *newargs)
            elif fname == 'upcast':
                newargs = [ self.create_function(args[0]), args[1] ]
                return functools.partial( self.upcast, *newargs)
            elif fname == 'input_filter':
                newargs = [ self.create_function(args[0]), args[1] ]
                if len(args) == 3:
                    newargs.append( self.create_function(args[2]))
                else:
                    newargs.append(None)
                return functools.partial( self.input_filter, *newargs)
            else:
                raise Exception("Function caster.{} does not exist".format(functiontext))
        else:
            fname = '.'.join(functiontext.split('~'))
            return operator.attrgetter(fname)(self.core)

    def __getattr__(self,attr):
        logger.debug("getattr: {}".format(attr))
        return self.create_function(attr)

