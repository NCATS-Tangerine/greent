from rdkit import Chem

from collections import defaultdict
parents = defaultdict(list)
with open('simpler.txt','r') as inf:
    for line in inf:
        x = line.strip().split('\t')
        parents[x[2]].append( (x[0], x[1]))

with open('parents.txt','w') as outf:
    for k,v in parents.items():
        ids = [ vi[0] for vi in v]
        smis = [ vi[1] for vi in v]
        smis2 = [ Chem.MolToSmiles(Chem.MolFromSmiles(vi[1])) for vi in v]
        if len(v) > 1:
            outf.write(f'{k}\t{len(v)}\n')
            for cid,smi in zip(ids,smis2):
                outf.write(f'\t{cid}\t{smi}\n')
            #if k not in smis:
            #    if k in smis2:
            #        print('parent not in original list, but is real')
            #    else:
            #        print('parent not in either list')
