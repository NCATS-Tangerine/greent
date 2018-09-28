from ftplib import FTP
from io import BytesIO
from gzip import decompress
from greent.util import LoggingUtil
import logging
import os
from crawler.mesh_unii import refresh_mesh_pubchem
from crawler.crawl_util import glom, dump_cache

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

def pull_via_ftp(ftpsite, ftpdir, ftpfile):
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    with BytesIO() as data:
        ftp.retrbinary(f'RETR {ftpfile}', data.write)
        binary = data.getvalue()
    ftp.quit()
    return binary

def pull(location,directory,filename):
    print(filename)
    data = pull_via_ftp(location, directory, filename)
    rdf = decompress(data).decode()
    return rdf



def make_mesh_id(mesh_uri):
    return f"mesh:{mesh_uri.split('/')[-1][:-1]}"


def load_chemicals(rosetta, refresh=False):
    #Build if need be
    if refresh:
        refresh_mesh_pubchem(rosetta)
    #Get all the simple stuff
    concord = load_unichem()
    #DO MESH/UNII
    mesh_unii_file = os.path.join(os.path.dirname(__file__),'mesh_to_unii.txt')
    mesh_unii_pairs = load_pairs(mesh_unii_file,'UNII')
    glom(concord,mesh_unii_pairs)
    #DO MESH/PUBCHEM
    mesh_pc_file = os.path.join(os.path.dirname(__file__),'mesh_to_pubchem.txt')
    mesh_pc_pairs = load_pairs(mesh_pc_file,'PUBCHEM')
    glom(concord,mesh_pc_pairs)
    #Dump
    with open('chemconc.txt','w') as outf:
        for key in concord:
            outf.write(f'{key}\t{concord[key]}\n')
#    dump_cache(concord,rosetta)

def load_pairs(fname,prefix):
    pairs = []
    with open(fname,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            mesh = f"MESH:{x[0]}"
            if x[1].startswith('['):
                pre_ids = x[1][1:-1].split(',')
                pre_ids = [pids.strip()[1:-1] for pids in pre_ids] #remove spaces and ' marks around ids
            else:
                pre_ids = [x[1]]
            for pid in pre_ids:
                if "'" in pid:
                    print("!")
                    print(pid)
                    print(fname)
                    print(prefix)
                    exit()
            ids = [ f'{prefix}:{pid}' for pid in pre_ids ]
            for identifier in ids:
                pairs.append( (mesh,identifier) )
    return pairs

def uni_glom(unichem_data,prefix1,prefix2,chemdict):
    print(f'{prefix1}/{prefix2}')
    n = unichem_data.split('\n')[1:]
    if len(n[-1]) == 0:
        n = n[:-1]
    pairs = [ ni.split('\t') for ni in n ]
    for p in pairs:
        if p[0].startswith("'") or p[1].startswith("'"):
            print('UNI_GLOM {prefix1} {prefix2} {p}')
    curiepairs = [ (f'{prefix1}:{p[0]}',f'{prefix2}:{p[1]}') for p in pairs]
    glom(chemdict,curiepairs)

'''
def glom(cpairs, chemdict):
    print(f'Starting with {len(chemdict)} entries')
    for cpair in cpairs:
        if cpair[0] not in chemdict:
            if cpair[1] in chemdict:
                chemdict[cpair[0]] = chemdict[cpair[1]]
            else:
                chemdict[cpair[0]] = set()
        if cpair[1] in chemdict:
            if chemdict[cpair[0]] != chemdict[cpair[1]]:
                chemdict[cpair[0]].update(chemdict[cpair[1]])
        chemdict[cpair[0]].update(cpair)
        chemdict[cpair[1]] = chemdict[cpair[0]]
    print(f'Ending with {len(chemdict)} entries')
'''

def load_unichem():
    chemcord = {}
    prefixes={1:'CHEMBL', 2:'DRUGBANK', 6:'KEGG.COMPOUND', 7:'CHEBI', 14:'UNII',  18:'HMDB', 22:'PUBCHEM'}
    #
    keys=list(prefixes.keys())
    keys.sort()
    for i in range(len(keys)):
        for j in range(i+1,len(keys)):
            print(i,j)
            ki = keys[i]
            kj = keys[j]
            prefix_i = prefixes[ki]
            prefix_j = prefixes[kj]
            dr =f'pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id{ki}'
            fl = f'src{ki}src{kj}.txt.gz'
            print(dr,fl)
            pairs = pull('ftp.ebi.ac.uk',dr ,fl )
            uni_glom(pairs,prefix_i,prefix_j,chemcord)
    return chemcord


