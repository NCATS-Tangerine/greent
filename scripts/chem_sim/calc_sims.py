from rdkit import DataStructs

def read():
    chems=[]
    with open('fp.txt','r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            ebv= DataStructs.ExplicitBitVect(2048)
            ebv.FromBase64(x[4])
            chem={'id': x[0],
                  'weight': float(x[3]),
                  'fp': ebv }
            chems.append(chem)
    return chems


def get_similar(chem,chems,thresh):
    matches=[]
    for c in chems:
        sim = DataStructs.FingerprintSimilarity(chem['fp'],c['fp'])
        if sim >= thresh:
            matches.append((sim,c))
    return matches

allchems = read()
print(len(allchems))

for i in range(100):
    sims = get_similar(allchems[i],allchems,0.90)
    print(allchems[i])
    print('---')
    for s in sims:
        print(s)
    print('+++++++++++++++++\n')
