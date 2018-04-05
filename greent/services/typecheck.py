from greent.service import Service
from greent.util import Text

class TypeCheck(Service):

    def __init__(self, context):
        super(TypeCheck, self).__init__("typecheck", context)

    def is_cell(self, node):
        """This is a very cheesy approach.  Once we have a generic ontology browser hooked in, we can reformulate"""
        curie_prefix = Text.get_curie(node.identifier)
        return curie_prefix == 'CL'
