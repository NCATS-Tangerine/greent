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

def refresh_mesh_pubchem():
    #Concept2mesh
    concept2mesh = parse_mesh(pull('ftp.nlm.nih.gov','/online/mesh/rdf', 'mesh.nt.gz'))
    print(len(concept2mesh))
    #synonym2mesh: read :synonym :is_synonym_of :concept
    #And some of the concepts are mesh (but some are not)
    synonym2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', 'pubchem/RDF/synonym', 'pc_synonym_topic.ttl.gz',concept2mesh,object_is_mesh,match_obj=True)
    print(len(synonym2mesh))
    #compound2mesh: read :synonym  :is_attribute_of :compound
    compounds2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/synonym', 'pc_synonym2compound_%06d.ttl.gz',synonym2mesh,true,match_obj=False)
    #You might think you could stop here, and let e.g. unichem link compounds to chebi/chembls.
    #but no.  synonym doesn't necessarily map to compound.  Let's dump out what we have
    print(len(compounds2mesh))
    with open('compound_mesh_1.txt','w') as outf:
        for key in compounds2mesh:
            outf.write(f'{key}\t{compounds2mesh[key]}\n')
    #now substance->synonym
    substance2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance2descriptor_%06d.ttl.gz',synonym2mesh,true,match_obj=True)
    print(len(substance2mesh))
    ### (the cid->everything else can come from unichem)
    compound2mesh_b = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance2compound_%06d.ttl.gz',substance2mesh,true,match_obj=False)
    print(len(compound2mesh_b))
    with open('compound_mesh_2.txt','w') as outf:
        for key in compound2mesh_b:
            outf.write(f'{key}\t{compound2mesh_b[key]}\n')
    #chembl2mesh: read
    #chembl2mesh = transfer_map('ftp.ncbi.nlm.nih.gov', '/pubchem/RDF/substance', 'pc_substance_match.ttl%06d.ttl.gz',substance2mesh,object_is_chembl,match_obj=False)

    #print(len(compounds2mesh))
    #with open('pubchemmesh.txt','w') as outf:
    #    for cid in compounds2mesh:
    #        for mesh in compounds2mesh[cid]:
    #            outf.write(f'{cid}\t{mesh}\n')
    #return compounds2mesh
