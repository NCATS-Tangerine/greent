from ftplib import FTP
from io import BytesIO
from json import loads
from gzip import decompress
from collections import defaultdict
from greent.graph_components import LabeledID
from greent.util import LoggingUtil
import logging

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

#### Get MeshID <- MeshConcept (D01045 <- M00141023)

def parse_mesh(mesh_data):
    concept2mesh = {}
    for line in mesh_data.split('\n'):
        if line.startswith('#'):
            continue
        triple = line[:-1].strip().split('\t')
        try:
            s,v,o = triple
        except:
            print(line)
            print( triple )
            continue
        if 'preferredConcept' in v:
            subject = make_mesh_id(s)
            object = make_mesh_id(o)
            concept2mesh[object] = subject
    return concept2mesh

def pull_mesh():
    data = pull_via_ftp('ftp.nlm.nih.gov','/online/mesh/rdf', 'mesh.nt.gz')
    mesh = parse_mesh(decompress(data).decode())
    return mesh

#### Get MeshConcept <- PC_Synonym
#### Make MeshID<-PC_Synonym

def parse_synonym_mesh(mdata,concept2id):
    synonyms2meshid=defaultdict(list)
    for line in mdata.split('\n'):
        if line.startswith('@'):
            continue
        if not 'mesh' in line:
            continue
        try:
            syn,v,meshconcept = line[:-1].strip().split('\t')
        except:
            print('bad line in parse_synonym_mesh')
            print(line)
            continue
        if meshconcept in concept2id:
            synonyms2meshid[syn].append(concept2id[meshconcept])
    return synonyms2meshid

def pull_pubchem_synonym_mesh(meshid2meshconcept):
    data = pull_via_ftp('ftp.ncbi.nlm.nih.gov', 'pubchem/RDF/synonym', 'pc_synonym_topic.ttl.gz')
    return parse_synonym_mesh( decompress(data).decode(), meshid2meshconcept)

##### PC_compounds -> PC_synonyms
#### Make PC_compounds->MeshID

def add_synonyms(compounds2mesh,data,synonyms2mesh):
    for line in data.split('\n'):
        if line.startswith('@'):
            continue
        try:
            syn,v,comp = line[:-1].strip().split('\t')
        except:
            print('bad line in synonym/compound')
            print(line)
            continue
        compounds2mesh[comp].extend(synonyms2mesh[syn])

def pull_pubchem_synonym_compounds(synonyms2mesh):
    ok = True
    n = 1
    compounds2mesh = defaultdict(list)
    while ok:
        try:
            data = pull_via_ftp('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/synonym', 'pc_synonym2compound_%06d.ttl.gz' % n)
        except:
            ok = False
        n+=1
        add_synonyms(compounds2mesh,decompress(data).decode(),synonyms2mesh)
    return compounds2mesh

##### PC_substances -> PC_compounds
##### Make PC_Compounds->MeshID

def add_compounds(substances2mesh,data,compounds2mesh):
    for line in data.split('\n'):
        if line.startswith('@'):
            continue
        substance,v,comp = line[:-1].strip().split('\t')
        substances2mesh[substance].extend(compounds2mesh[comp])

def pull_pubchem_compounds_substances(compounds2mesh):
    ok = True
    n = 1
    substances2mesh = defaultdict(list)
    while ok:
        try:
            data = pull_via_ftp('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance2compound_%06d.ttl.gz' % n)
        except:
            ok = False
        n+=1
        add_compounds(substances2mesh,decompress(data).decode(),compounds2mesh)


##### CHEMBL -> PC_substances
##### Make CHEMBL->MeshID

def parse_match(data,substance2meshid):
    chembl2mesh = defaultdict(list)
    for line in data.split('\n'):
        if not line.startswith('substance'):
            continue
        triple = line[:-1].strip().split('\t')
        substance,v,chembl = triple
        chembl2mesh[chembl].extend(substance2meshid[substance])
    return chembl2mesh

def pull_pc_chembl(substance2meshid):
    data = pull_via_ftp('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance_match.ttl.gz' % n)
    mesh = parse_match(decompress(data).decode(),substance2meshid)
    return mesh

def make_mesh_id(mesh_uri):
    return f"mesh:{mesh_uri.split('/')[-1][:-1]}"

def load_chemicals(rosetta):
    concept2mesh = pull_mesh()
    synonym2mesh = pull_pubchem_synonym_mesh(concept2mesh)
    compounds2mesh = pull_pubchem_synonym_compounds(synonym2mesh)
    substance2mesh = pull_pubchem_compounds_substances(compounds2mesh)
    chembl2mesh = pull_pc_chembl(substance2mesh)
    print(len(chembl2mesh))
    with open('chemblmesh.txt','w') as outf:
        for chembl in chembl2mesh:
            for mesh in chembl2mesh[chembl]:
                outf.write(f'{chembl}\t{mesh}')
