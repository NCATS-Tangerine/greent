from ftplib import FTP
from io import BytesIO
from gzip import decompress, GzipFile
from greent.util import LoggingUtil, Text
from greent.graph_components import LabeledID
import logging
import os
from crawler.mesh_unii import refresh_mesh_pubchem
from crawler.crawl_util import glom, dump_cache, pull_via_ftp
from functools import partial
import pickle
import requests

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)


def pull(location,directory,filename):
    data = pull_via_ftp(location, directory, filename)
    rdf = decompress(data).decode()
    return rdf

def make_mesh_id(mesh_uri):
    return f"mesh:{mesh_uri.split('/')[-1][:-1]}"

def pull_mesh_chebi():
    url = 'https://query.wikidata.org/sparql?format=json&query=SELECT ?chebi ?mesh WHERE { ?compound wdt:P683 ?chebi . ?compound wdt:P486 ?mesh. }'
    results = requests.get(url).json()
    pairs = [ (f'MESH:{r["mesh"]["value"]}',f'CHEBI:{r["chebi"]["value"]}')
             for r in results['results']['bindings']
             if not r['mesh']['value'].startswith('M') ]
    with open('mesh_chebi.txt','w') as outf:
        for m,c in pairs:
            outf.write(f'{m}\t{c}\n')
    return pairs

def load_chemicals(rosetta, refresh=False):
    #Build if need be
    if refresh:
        refresh_mesh_pubchem(rosetta)
    #Get all the simple stuff
    print('UNICHEM')
    concord = load_unichem()
    #DO MESH/UNII
    print('MESH/UNII')
    mesh_unii_file = os.path.join(os.path.dirname(__file__),'mesh_to_unii.txt')
    mesh_unii_pairs = load_pairs(mesh_unii_file,'UNII')
    glom(concord,mesh_unii_pairs)
    #DO MESH/PUBCHEM
    print('MESH/PUBCHEM')
    mesh_pc_file = os.path.join(os.path.dirname(__file__),'mesh_to_pubchem.txt')
    mesh_pc_pairs = load_pairs(mesh_pc_file,'PUBCHEM')
    glom(concord,mesh_pc_pairs)
    #DO MESH/CHEBI, but don't combine any chebi's into a set with it
    print('MESH/CHEBI')
    mesh_chebi = pull_mesh_chebi()
    glom(concord, mesh_chebi,['CHEBI'])
    #Add labels to CHEBIs, CHEMBLs, and MESHes
    print('LABEL')
    label_chebis(concord)
    label_chembls(concord)
    label_meshes(concord)
    #Dump
    with open('chemconc.txt','w') as outf:
        for key in concord:
            outf.write(f'{key}\t{concord[key]}\n')
    dump_cache(concord,rosetta)

def get_chebi_label(ident):
    res = requests.get(f'http://onto.renci.org/label/{ident}/').json()
    return res['label']

def get_chembl_label(ident):
    res = requests.get(f'https://www.ebi.ac.uk/chembl/api/data/molecule/{Text.un_curie(ident)}.json').json()
    return res['pref_name']

def get_dict_label(ident,labels):
    try:
        return labels[ident]
    except KeyError:
        return None

def get_mesh_label(ident,labels):
    return labels[Text.un_curie(ident)]

###

def label_chebis(concord):
    print('READ CHEBI')
    chebiobo = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/ontology','chebi_lite.obo' ).decode()
    lines = chebiobo.split('\n')
    chebi_labels = {}
    for line in lines:
        if line.startswith('[Term]'):
            tid = None
            label = None
        elif line.startswith('id:'):
            tid = line[3:].strip()
        elif line.startswith('name:'):
            label = line[5:].strip()
            chebi_labels[tid] = label
    print('LABEL CHEBI')
    label_compounds(concord, 'CHEBI', partial(get_dict_label,labels=chebi_labels))
    #label_compounds(concord,'CHEBI',get_chebi_label)

def process_chunk(lines,label_dict):
    if len(lines) == 0:
        return
    if not lines[0].startswith('chembl_molecule'):
        return
    chemblid = f"CHEMBL:{lines[0].split()[0].split(':')[1]}"
    label = None
    for line in lines[1:]:
        s = line.strip()
        if s.startswith('rdfs:label'):
            label = s.split()[1]
            if label.startswith('"'):
                label = label[1:]
            if label.endswith('"'):
                label = label[:-1]
    if label is not None:
        label_dict[chemblid] = label

def label_chembls(concord):
    print('READ CHEMBL')
    fname ='chembl_24.1_molecule.ttl.gz'
    #uncomment if you need a new one
    #data=pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/24.1/',fname)
    #with open(fname,'wb') as outf:
    #    outf.write(data)
    chembl_labels = {}
    chunk = []
    with GzipFile(fname,'r') as inf:
        for line in inf:
            l = line.decode().strip()
            if len(l) == 0:
                process_chunk(chunk,chembl_labels)
                chunk = []
            elif l.startswith('@'):
                pass
            else:
                chunk.append(l)
    print('LABEL CHEMBL',len(chembl_labels))
    label_compounds(concord, 'CHEMBL', partial(get_dict_label,labels=chembl_labels))
    #label_compounds(concord,'CHEMBL',get_chembl_label)

def label_meshes(concord):
    print('LABEL MESH')
    labelname = os.path.join(os.path.dirname(__file__), 'meshlabels.pickle')
    with open(labelname,'rb') as inf:
        mesh_labels = pickle.load(inf)
    label_compounds(concord, 'MESH', partial(get_mesh_label,labels=mesh_labels))

###

def label_compounds(concord,prefix,get_label):
    foundlabels = {}
    for k,v in concord.items():
        to_remove = []
        to_add = []
        for ident in v:
            if Text.get_curie(ident) == prefix:
                if not ident in foundlabels:
                    label = get_label(ident)
                    if label is not None:
                        lid = LabeledID(ident, get_label(ident))
                        foundlabels[ident] = lid
                    else:
                        foundlabels[ident] = None
                label = foundlabels[ident]
                if label is not None:
                    to_remove.append(ident)
                    to_add.append(foundlabels[ident])
        for r in to_remove:
            v.remove(r)
        for r in to_add:
            v.add(r)

def remove_ticks(s):
    if s.startswith("'"):
        s = s[1:]
    if s.endswith("'"):
        s = s[:-1]
    return s

def load_pairs(fname,prefix):
    pairs = []
    with open(fname,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            mesh = f"MESH:{x[0]}"
            if x[1].startswith('['):
                pre_ids = x[1][1:-1].split(',')
                pre_ids = [remove_ticks(pids.strip()) for pids in pre_ids] #remove spaces and ' marks around ids
            else:
                pre_ids = [x[1]]
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

def load_unichem():
    chemcord = {}
    prefixes={1:'CHEMBL', 2:'DRUGBANK', 6:'KEGG.COMPOUND', 7:'CHEBI', 14:'UNII',  18:'HMDB', 22:'PUBCHEM'}
    #
    keys=list(prefixes.keys())
    keys.sort()
    for i in range(len(keys)):
        for j in range(i+1,len(keys)):
            ki = keys[i]
            kj = keys[j]
            prefix_i = prefixes[ki]
            prefix_j = prefixes[kj]
            dr =f'pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id{ki}'
            fl = f'src{ki}src{kj}.txt.gz'
            pairs = pull('ftp.ebi.ac.uk',dr ,fl )
            uni_glom(pairs,prefix_i,prefix_j,chemcord)
    return chemcord


