from greent.rosetta import Rosetta
from greent import node_types

def write_results(outf,ti,tj,results):
    print(f'{ti}->{tj}')
    used = set()
    for result in results:
        p = result['p']
        if p in used:
            continue
        plabel = result['pLabel']
        used.add(p)
        outf.write(f'{ti}\t{tj}\t{p}\t{plabel}\n')

def create_metamap():
    rosetta = Rosetta()
    uberon = rosetta.core.uberongraph
    types = [node_types.DISEASE,
             node_types.MOLECULAR_ACTIVITY,
             node_types.BIOLOGICAL_PROCESS,
             node_types.PHENOTYPIC_FEATURE,
             node_types.CELL,
             node_types.ANATOMICAL_ENTITY,
             node_types.CHEMICAL_SUBSTANCE]
    with open('ubergraph_metamap.txt','w') as outf:
        outf.write('sourcetype\tobjecttype\trelation_id\trelation_label\n')
        for i,ti in enumerate(types):
            for j,tj in enumerate(types[i:]):
                results = uberon.get_edges(ti,tj)
                write_results(outf,ti,tj,results)
                if not j == 0:
                    results = uberon.get_edges(tj,ti)
                    write_results(outf,tj,ti,results)

if __name__ == '__main__':
    create_metamap()


