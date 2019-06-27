from rdkit import DataStructs
from rdkit import Chem
from rdkit.Chem.Fingerprints import FingerprintMols
from rdkit.Chem.Descriptors import HeavyAtomMolWt

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
        mol = Chem.MolFromSmiles(chem['smiles'])
        try:
            chem['hamw'] = HeavyAtomMolWt(mol)
            chem['fp'] = FingerprintMols.FingerprintMol(mol).ToBitString()
            #chems.append(chem)
            outf.write(f"{chem['id']}\t{chem['smiles']}\t{chem['hamw']}\t{chem['fp']}\n")
        except Exception as e:
            print(f"error with {x}")
