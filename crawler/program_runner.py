from builder.question import LabeledID
from greent import node_types
from builder.buildmain import run
from multiprocessing import Pool
from functools import partial

def get_identifiers(input_type,rosetta):
    lids = []
    if input_type == node_types.DISEASE:
        identifiers = rosetta.core.mondo.get_ids()
        for ident in identifiers:
            if ident != 'MONDO:0000001': # MONDO:1 == disease  Don't want the root node. We'll keep the others?
                label = rosetta.core.mondo.get_label(ident)
                if label is not None and not label.startswith('obsolete'):
                    lids.append(LabeledID(ident,label))
    else:
        print(f'Not configured for input type: {input_type}')
    return lids


def do_one(itype,otype,identifier):
    print(identifier.identifier)
    path = f'{itype},{otype}'
    run(path,identifier.label,identifier.identifier,None,None,None,'greent.conf')

def load_all(input_type, output_type,rosetta,poolsize):
    """Given an input type and an output type, run a bunch of workflows dumping results into neo4j and redis"""
    identifiers = get_identifiers(input_type,rosetta)
    print( f'Found {len(identifiers)} input {input_type}')
    partial_do_one = partial(do_one, input_type, output_type)
    pool = Pool(processes=poolsize)
    pool.map(partial_do_one, identifiers)
    pool.close()
    pool.join()
