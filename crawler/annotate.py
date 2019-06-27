import argparse
from greent.rosetta import Rosetta
from greent.annotators.annotator_factory import annotator_class_list
from builder.question import LabeledID
from builder.buildmain import run
from multiprocessing import Pool
from functools import partial


def grab_all(node_type, rosetta):
    """
    Grabs ids and labels of all the node if type node_type.
    """
    with rosetta.type_graph.driver.session() as session:
        return session.run(f"MATCH (c:{args.annotate}) return c.id as id , c.name as label")
def run_wrapper(node_type, lids):
    print('starting builder to annotate nodes...')
    print(len(lids))
    run(f'{args.annotate}, XXXXXXX','','',None, None, None, 'greent.conf', identifier_list = lids)
def start(args) :
    if args.annotate:
        rosetta = Rosetta()
        if args.annotate in annotator_class_list:
            print('starting annotation and synonmization')
            results = grab_all(args.annotate, rosetta)
            lids = [LabeledID(x['id'],x['label']) for x in results]
            pool_size = 10
            chunks = pool_size * 2
            chunksize = int(len(lids)/chunks)
            single_run_size = chunksize if chunksize > 0 else 1 
            lids_chunks = [lids[i: i+ single_run_size] for i in range(0, len(lids),single_run_size)]
            partial_run = partial(run_wrapper,f'{args.annotate}')
            print('starting processes')
            pool = Pool(processes = pool_size)
            pool.map_async(partial_run, lids_chunks, error_callback = lambda error: print(error))  
            pool.close()
            pool.join() 
            print('done.')
        else: 
            raise Exception(f'No annotator found for {args.annotate}')
    else:
        raise Exception('No argument passed.')
    
if __name__ == '__main__':
    
    helpstring = f"""
    A tool that allows annotating all nodes in the database of certain type. Currently allowed types are : 
    {annotator_class_list.keys()}. 
    """
    parser = argparse.ArgumentParser(description=helpstring,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-a','--annotate', help='Types of nodes to annotate.')

    args = parser.parse_args()
    start (args)