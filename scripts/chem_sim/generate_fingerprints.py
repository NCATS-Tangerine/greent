from rdkit import DataStructs
from rdkit import Chem
from rdkit.Chem.Fingerprints import FingerprintMols
from rdkit.Chem.Descriptors import HeavyAtomMolWt
from rdkit.Chem.SaltRemover import SaltRemover


#Putting "O" in here will unify hydrates, like morphine and morphine monohydrate (called morphine by mesh!)
remover = SaltRemover(defnData='[Cl,Br,K,I,Na,O]')

chems = []
n = 0
with open('smiles.txt','r') as inf, open('fp.txt','w') as outf:
    for line in inf:
        n+=1
        if n % 10000 == 0:
            print(n)
        x = line.strip().split('\t')
        chem = {'id': x[0],
                'smiles': x[2]}
        try:
            mol = Chem.MolFromSmiles(chem['smiles'])
            res = remover.StripMol(mol)
            desaltsmi = Chem.MolToSmiles(res)
            chem['hamw'] = HeavyAtomMolWt(res)
            chem['fp'] = FingerprintMols.FingerprintMol(res).ToBase64()
            #chems.append(chem)
            outf.write(f"{chem['id']}\t{chem['smiles']}\t{desaltsmi}\t{chem['hamw']}\t{chem['fp']}\n")
        except Exception as e:
            print(f"error with {x}")
