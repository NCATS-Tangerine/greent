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
'''
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
'''

def make_mesh_id(mesh_uri):
    return f"mesh:{mesh_uri.split('/')[-1][:-1]}"

#This one is a little different because we're not attaching to an old dict
# and there's some parsing of the mesh ids that has to happen as well
def parse_mesh(data):
    concept2mesh = defaultdict(set)
    for line in data.split('\n'):
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
            concept2mesh[object].add(subject)
    return concept2mesh

def transfer(x2mesh,y2mesh,rdf,condition,match_obj):
    continuing = False
    for line in rdf.split('\n'):
        if len(line) == 0:
            continue
        if line.startswith('@'):
            continue
        if not continuing:
            try:
                subject,predicate,obj = line[:-1].strip().split('\t')
            except:
                print('fail')
                print(line)
                exit()
        else:
            #subj, pred are same as previous line
            obj = line[:-1].strip()
        if line.endswith(','):
            continuing = True
        else:
            continuing = False
        if not condition(subject,predicate,obj):
            continue
        if match_obj:
            xkey = obj
            other = subject
        else:
            xkey = subject
            other = obj
        if xkey in x2mesh:
            y2mesh[other].update(x2mesh[xkey])
    return y2mesh


def pull(location,directory,filename):
    print(filename)
    data = pull_via_ftp(location, directory, filename)
    rdf = decompress(data).decode()
    return rdf

def transfer_map(loc, dirname, fname, x2mesh, condition, match_obj=True):
    y2mesh = defaultdict(set)
    if '%' not in fname:
        rdf = pull(loc,dirname,fname)
        transfer(x2mesh,y2mesh,rdf,condition,match_obj)
    else:
        ok = True
        n = 1
        while ok:
            try:
                rdf = pull(loc,dirname,fname % n)
            except:
                ok = False
            n+=1
            transfer(x2mesh,y2mesh,rdf,condition,match_obj)
    return y2mesh

def object_is_mesh(s,p,o):
    return o.startswith('mesh:')

def object_is_chembl(s,p,o):
    return o.startswith('chembl:')

def true(s,p,o):
    return True

def load_chemicals(rosetta):
    #load_unichem()
    cid2mesh = get_pubchem_cid_mesh()

def uni_glom(unichem_data,prefix1,prefix2,chemdict):
    n = unichem_data.split('\n')[1:]
    pairs = [ ni.split('\t') for ni in n ]
    curiepairs = [ (f'{prefix1}:{p[0]}',f'{prefix2}:{p[1]}') for p in pairs]
    glom(curiepairs,chemdict)

def glom(cpairs, chemdict):
    for cpair in cpairs:
        if cpair[0] not in chemdict:
            if cpair[1] in chemdict:
                print('hmmm')
                print(cpair[0],cpair[1])
                exit()
            chemdict[cpair[0]] = set()
        #for curie in cset:
            #if curie

def load_unichem():
    chemcord = {}
    chembl_db = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1', 'src1src2.txt.gz')
    uni_glom(chembl_db,'CHEMBL','DRUGBANK',chemcord)
    chembl_chebi = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1', 'src1src7.txt.gz')
    uni_glom(chembl_chebi,'CHEMBL','CHEBI',chemcord)
    chembl_pubchem = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1', 'src1src22.txt.gz')
    uni_glom(chembl_pubchem,'CHEMBL','PUBCHEM',chemcord)
    db_chebi = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id2', 'src2src7.txt.gz')
    db_pubchem = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id2', 'src2src22.txt.gz')
    chebi_pubchem = pull('ftp.ebi.ac.uk','pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id7', 'src7src22.txt.gz')

def get_pubchem_cid_mesh():
    #Concept2mesh
    concept2mesh = parse_mesh(pull('ftp.nlm.nih.gov','/online/mesh/rdf', 'mesh.nt.gz'))
    print(len(concept2mesh))
    #synonym2mesh: read :synonym :is_synonym_of :concept
    #And some of the concepts are mesh (but some are not)
    synonym2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', 'pubchem/RDF/synonym', 'pc_synonym_topic.ttl.gz',concept2mesh,object_is_mesh,match_obj=True)
    print(len(synonym2mesh))
    #compound2mesh: read :synonym  :is_attribute_of :compound
    compounds2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/synonym', 'pc_synonym2compound_%06d.ttl.gz',synonym2mesh,true,match_obj=False)
    print(len(compounds2mesh))

    ###If you want to go all the way to chembl2mesh, you can, but all we really need is pubchem cid to mesh
    ### (the cid->everything else can come from unichem)
    #substance2mesh: read :substance :has :compound
    #substance2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance2compound_%06d.ttl.gz',compounds2mesh,true,match_obj=True)
    #chembl2mesh: read
    #chembl2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance_match.ttl%06d.ttl.gz',substance2mesh,object_is_chembl,match_obj=False)

    print(len(compounds2mesh))
    with open('pubchemmesh.txt','w') as outf:
        for cid in compounds2mesh:
            for mesh in compounds2mesh[cid]:
                outf.write(f'{cid}\t{mesh}\n')
    return compounds2mesh
