import logging
#from program import Program
from greent.node_types import node_types, UNSPECIFIED
from greent.util import Text, LoggingUtil
from greent.program import Program
from greent.program import QueryDefinition

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class Transition:
    def __init__(self, last_type, next_type, min_path_length, max_path_length):
        self.in_type = last_type
        self.out_type = next_type
        self.min_path_length = min_path_length
        self.max_path_length = max_path_length
        self.in_node = None
        self.out_node = None

    def generate_reverse(self):
        return Transition(self.out_type, self.in_type, self.min_path_length, self.max_path_length)

    @staticmethod
    def get_fstring(ntype):
        if ntype is None:
            return 'n{0}'
        else:
            return 'n{0}:{1}'

    def generate_concept_cypher_pathstring(self, t_number):
        pstring = f'MATCH p{t_number}='
        if t_number == 0:
            start = f'(c{t_number}:Concept {{name: "{self.in_type}" }})\n'
            pstring += start
        else:
            start = f'(c{t_number})\n'
            pstring += start
        if self.max_path_length > 1:
            pstring += f'-[:translation*{self.min_path_length}..{self.max_path_length}]-\n'
        else:
            pstring += '--\n'
        end = f'(c{t_number+1}:Concept {{name: "{self.out_type}" }})'
        pstring += end
        return pstring

class UserQuery:
    """This is the class that the rest of builder uses to interact with a query."""

    def __init__(self, start_values, start_type, start_name):
        """Create an instance of UserQuery. Takes a starting value and the type of that value"""
        self.query = None
        self.definition = QueryDefinition()
        # Value for the original node
        self.definition.start_values = start_values
        self.definition.start_type = start_type
        self.definition.start_name = start_name
        self.definition.end_values = None
        self.definition.end_name = None
        # List of user-level types that we must pass through
        self.add_node(start_type)

    def add_node(self, node_type):
        """Add a node to the node list, validating the type
           20180108: node_type may be None"""
        # Our start node is more specific than this...  Need to have another validation method
        if node_type is not None and node_type not in node_types:
            raise Exception('node type must be one of greent.node_types')
        self.definition.node_types.append(node_type)

    def add_transition(self, next_type, min_path_length=1, max_path_length=1, end_values=None, end_name=None):
        """Add another required node type to the path.

        When a new node is added to the user query, the user is asserting that
        the returned path must go through a node of this type.  The default is
        that the next node should be directly related to the previous. That is,
        no other node types should be between the previous node and the current
        node.   There may be other nodes, but they will represent synonyms of
        the previous or current node.  This is defined using the
        max_path_length input, which defaults to 1.  On the other hand, a user
        may wish to define that some number of other node types must be between
        one node and another.  This can be specified by the min_path_length,
        which also defaults to 1.  If indirect edges are demanded, this
        parameter is set higher.  If this is the final transition, a value for
        the terminal node may be added.  Attempting to add more transitions
        after setting an end value will result in an exception.  If this is the
        terminal node, but it does not have a specified value, then no
        end_value needs to be specified.

        arguments: next_type: type of the output node from the transition.
                              Must be an element of reasoner.node_types.
                   min_path_length: The minimum number of non-synonym transitions
                                    to get from the previous node to the added node
                   max_path_length: The maximum number of non-synonym transitions to get
                                    from the previous node to the added node
                   end_value: Value of this node (if this is the terminal node, otherwise None)
        """
        # validate some inputs
        # TODO: subclass Exception
        if min_path_length > max_path_length:
            raise Exception('Maximum path length cannot be shorter than minimum path length')
        if self.definition.end_values:
            raise Exception('Cannot add more transitions to a path with a terminal node')
        # Add the node to the type list
        self.add_node(next_type)
        # Add the transition
        t = Transition(self.definition.node_types[-2], next_type, min_path_length, max_path_length)
        self.definition.transitions.append(t)
        # Add the end_value
        if end_values:
            self.definition.end_values = end_values
            self.definition.end_name = end_name

    def generate_cypher(self):
        """Generate a cypher query to find paths through the concept-level map."""
        cypherbuffer = []
        paths_parts = []
        for t_number, transition in enumerate(self.definition.transitions):
            paths_parts.append(transition.generate_concept_cypher_pathstring(t_number))
        cypherbuffer.append( '\n'.join(paths_parts) )
        last_node_i = len(self.definition.transitions)
        nodes = '+'.join([f'nodes(p{i})' for i in range(len(self.definition.transitions))])
        relationships = '+'.join([f'relationships(p{i})' for i in range(len(self.definition.transitions))])
        if self.definition.end_values is None:
            cypherbuffer.append(f"WHERE robokop.traversable({nodes}, {relationships}, [c0])")
        else:
            cypherbuffer.append(f'WHERE robokop.traversable({nodes}, {relationships}, [c0,c{last_node_i}])')
        #This is to make sure that we don't get caught up in is_a and other funky relations.:
        cypherbuffer.append(f'AND ALL( r in {relationships} WHERE  EXISTS(r.op) )')
        cypherbuffer.append(f"RETURN {', '.join([f'p{i}' for i in range(len(self.definition.transitions))])}")
        return '\n'.join(cypherbuffer)

    def compile_query(self, rosetta):
        self.cypher = self.generate_cypher()
        logger.debug(self.cypher)
        plans = rosetta.type_graph.get_transitions(self.cypher)
        #self.programs = [Program(plan, self.definition, rosetta, i) for i,plan in enumerate(plans)]
        self.programs = []
        for i,plan in enumerate(plans):
            try:
                #Some programs are bogus (when you have input to a named node) 
                #it throws an exception then, and we ignore it.
                self.programs.append(Program(plan, self.definition, rosetta, i))
            except Exception as err:
                logger.warn(f'WARN: {err}')
        return len(self.programs) > 0

    def get_programs(self):
        return self.programs

    def get_terminal_nodes(self):
        starts = set()
        ends = set()
        return self.query.get_terminal_types()

