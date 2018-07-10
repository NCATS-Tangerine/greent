from ftplib import FTP
from io import BytesIO
from json import loads
from gzip import decompress
from collections import defaultdict
from greent.graph_components import LabeledID
from greent.util import LoggingUtil
import logging
from crawler.mesh_pubchem import refresh_mesh_pubchem

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


def load_chemicals(rosetta,refresh=False):
    if refresh:
        refresh_mesh_pubchem()
    concord = load_unichem()
    cid2mesh_1 = load_pubchem_mesh('compound_mesh_1.txt')
    print('PUBCHEM/MESH')
    glom(cid2mesh_1, concord)
    cid2mesh_2 = load_pubchem_mesh('compound_mesh_2.txt')
    print('PUBCHEM/MESH')
    glom(cid2mesh_2, concord)
    with open('chemconc.txt','w') as outf:
        for key in concord:
            outf.write(f'{key}\t{concord[key]}\n')
    for chem_id in concord:
        key = f"synonymize({chem_id})"
        value = concord[chem_id]
        rosetta.cache.set(key,value)

def load_pubchem_mesh(fname):
    pairs = []
    with open(fname,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            pc = f"PUBCHEM:{x[0].split(':')[1][3:]}"
            pre_meshes = x[1][1:-1].split(',')
            meshes = [ f"MESH:{pmesh.strip()[1:-1].split(':')[1]}" for pmesh in pre_meshes ]
            for mesh in meshes:
                pairs.append( (pc,mesh) )
    return pairs

def uni_glom(unichem_data,prefix1,prefix2,chemdict):
    print(f'{prefix1}/{prefix2}')
    n = unichem_data.split('\n')[1:]
    if len(n[-1]) == 0:
        n = n[:-1]
    pairs = [ ni.split('\t') for ni in n ]
    curiepairs = [ (f'{prefix1}:{p[0]}',f'{prefix2}:{p[1]}') for p in pairs]
    glom(curiepairs,chemdict)

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

def load_unichem():
    chemcord = {}
    chembl_db = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1', 'src1src2.txt.gz')
    uni_glom(chembl_db,'CHEMBL','DRUGBANK',chemcord)
    chembl_chebi = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1', 'src1src7.txt.gz')
    uni_glom(chembl_chebi,'CHEMBL','CHEBI',chemcord)
    chembl_pubchem = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1', 'src1src22.txt.gz')
    uni_glom(chembl_pubchem,'CHEMBL','PUBCHEM',chemcord)
    db_chebi = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id2', 'src2src7.txt.gz')
    uni_glom(db_chebi,'DRUGBANK','CHEBI',chemcord)
    db_pubchem = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id2', 'src2src22.txt.gz')
    uni_glom(db_pubchem,'DRUGBANK','PUBCHEM',chemcord)
    chebi_pubchem = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id7', 'src7src22.txt.gz')
    uni_glom(chebi_pubchem,'CHEBI','PUBCHEM',chemcord)
    return chemcord


