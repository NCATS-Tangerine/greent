from ply import lex
from collections import namedtuple
from greent import node_types 

tokens = (
    "NODE",
    "EDGE"
)

t_NODE = (r"S|G|P|C|A|D|X|T|W|\?")

def t_EDGE(t):
    r"\(\d+\-\d+\)"
    #t.value = int(t.value)
    return t

def t_error(t):
    raise TypeError("Unknown text '%s'" % (t.value,))

lex.lex()

def tokenize_path(path):
    lex.input(path)
    steps = []
    Step = namedtuple('Step', ['nodetype', 'min_path_length', 'max_path_length' ] )
    mm = [1,1]
    end_ok = False
    for tok in iter(lex.token, None):
        if tok.type == 'NODE':
            ntype = node_types.type_codes[ tok.value ]
            steps.append( Step( ntype, mm[0], mm[1] ) )
            mm = [1,1]
            end_ok = True
        elif tok.type == 'EDGE':
            mm = [int(x) for x in tok.value[1:-1].split('-')]
            end_ok = False
    if not end_ok:
        raise ValueError ('Pathway cannot end with unknown node types')
    return steps

def test():
    path = tokenize_path("S(1-3)DG")
    print( path )

if __name__ == '__main__':
    test()

