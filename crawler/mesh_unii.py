from gzip import decompress
from greent.util import LoggingUtil
import itertools
import logging
import requests
import os
import pickle

from crawler.crawl_util import pull_via_ftp

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)


def pull(location,directory,filename):
    print(filename)
    data = pull_via_ftp(location, directory, filename)
    rdf = decompress(data).decode()
    with open(filename,'w') as outf:
        outf.write(rdf)
    return rdf

def parse_mesh(data):
    """THERE are two kinds of mesh identifiers that correspond to chemicals.
    1. Anything in the D tree
    2. SCR_Chemicals from the appendices.
    Dig through and find anything like this"""
    chemical_mesh = set()
    unmapped_mesh = set()
    term_to_concept = {}
    concept_to_cas  = {}
    concept_to_unii  = {}
    concept_to_EC  = {}
    concept_to_label = {}
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
        if v == '<http://id.nlm.nih.gov/mesh/vocab#treeNumber>':
            treenum = o.split('/')[-1]
            if treenum.startswith('D'):
                meshid = s[:-1].split('/')[-1]
                chemical_mesh.add(meshid)
        elif o == '<http://id.nlm.nih.gov/mesh/vocab#SCR_Chemical>':
            meshid = s[:-1].split('/')[-1]
            chemical_mesh.add(meshid)
        elif v == '<http://id.nlm.nih.gov/mesh/vocab#preferredConcept>':
            meshid = s[:-1].split('/')[-1]
            concept = o
            term_to_concept[meshid] = o
        elif v == '<http://id.nlm.nih.gov/mesh/vocab#registryNumber>':
            o = o[1:-1] #Strip quotes
            if o == '0':
                continue
            if '-' in o:
                concept_to_cas[s] = o
            elif o.startswith('EC'):
                concept_to_EC[s] = o
            else:
                concept_to_unii[s] = o
        elif v == '<http://www.w3.org/2000/01/rdf-schema#label>':
            meshid = s[:-1].split('/')[-1]
            concept_to_label[meshid] = o.strip().split('"')[1]
    term_to_cas={}
    term_to_unii={}
    term_to_EC={}
    for term,concept in term_to_concept.items():
        if concept in concept_to_cas:
            term_to_cas[term] = concept_to_cas[concept]
        elif concept in concept_to_unii:
            term_to_unii[term] = concept_to_unii[concept]
        elif concept in concept_to_EC:
            term_to_EC[term] = concept_to_EC[concept]
        else:
            unmapped_mesh.add(term)
    print ( f"Found {len(chemical_mesh)} compounds in mesh")
    print ( f"Found {len(term_to_cas)} compounds with CAS identifiers")
    print ( f"Found {len(term_to_unii)} compounds with UNII identifiers")
    print ( f"Found {len(unmapped_mesh)} compounds with NOTHING")
    print ( f"{len(term_to_cas) + len(term_to_unii) + len(unmapped_mesh)}")
    return unmapped_mesh, term_to_cas, term_to_unii, term_to_EC,concept_to_label

def dump(outdict,outfname):
    with open(outfname,'w') as outf:
        for mesh,unii in outdict.items():
            outf.write(f'{mesh}\t{unii}\n')

def chunked(it, size):
    """Wraps an iterable, returning it in chunks of size: size"""
    it = iter(it)
    while True:
        p = tuple(itertools.islice(it, size))
        if not p:
            break
        yield p

def lookup_by_mesh(meshes,apikey):
    term_to_pubs = {}
    if apikey is None:
        print('Warning: not using API KEY for eutils, resulting in 3x slowdown')
    chunksize=10
    backandforth={'C': '67', '67': 'C', 'D': '68', '68': 'D'}
    for terms in chunked(meshes,chunksize):
        url='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?&dbfrom=mesh&db=pccompound&retmode=json'
        if apikey is not None:
            url+=f'&apikey={apikey}'
        for term in terms:
            try:
                newterm = f'{backandforth[term[0]]}{term[1:]}'
            except KeyError:
                #Q terms get in here, which are things like "radiotherapy"
                continue
            url+=f'&id={newterm}'
        try:
            result = requests.get(url).json()
        except Exception as e:
            print(url)
            print(result)
            print(e)
            exit()
        if 'linksets' not in result:
            continue
        linksets = result['linksets']
        for ls in linksets:
            cids = None
            if 'linksetdbs' in ls:
                mesh=ls['ids'][0]
                for lsdb in ls['linksetdbs']:
                    if lsdb['linkname'] == 'mesh_pccompound':
                        cids = lsdb['links']
            if cids is not None:
                smesh = str(mesh)
                remesh = f'{backandforth[smesh[0:2]]}{smesh[2:]}'
                if len(cids) <5:
                    # 5 or more is probably a group, not a compound
                    term_to_pubs[remesh] = cids
    print(f'mesh found {len(term_to_pubs)}')
    return term_to_pubs

def lookup_by_cas(term_to_cas,apikey):
    term_to_pubs = {}
    if apikey is None:
        print('Warning: not using API KEY for eutils, resulting in 3x slowdown')
    for term in term_to_cas:
        cas = term_to_cas[term]
        url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pccompound&term={cas}&retmode=json'
        if apikey is not None:
            url+=f'&apikey={apikey}'
        try:
            response = requests.get(url).json()
        except Exception as e:
            print(e)
            print(url)
            continue
        try:
            r = response['esearchresult']
            if 'errorlist' in r:
                if 'phrasesnotfound' in r['errorlist']:
                    if cas in r['errorlist']['phrasesnotfound']:
                        continue
            term_to_pubs[term] = response['esearchresult']['idlist']
        except:
            continue
    print(f'cas found {len(term_to_pubs)}')
    return term_to_pubs

def refresh_mesh_pubchem(rosetta):
    """There are 3 possible ways to map mesh terms
    1. Sometimes the registry term in mesh will be a UNII id.  These are great, unichem can map them to everything else.
    2. Sometimes there is a CAS number. These are ok. It's a good way to get a less ambiguous mapping, but you have to
       use eutils to get at them.  Furthermore: A single CAS will map to multiple PUBCHEM compounds.  This is apparently
       because somebody is not paying attention to stereochemistry.  I'm not sure if it's CAS or PUBCHEM mapping to CAS
       but the upshot is that there is no way to choose which pubchem we want, so we will take all, and that will
       end up glomming together stereo and non-stereo versions  fo the structure. Oh well.
    3. Sometimes the registry term is 0.  Literally.  In this case, the only hope is to call eutils and see what you
       get back.  Here's what NLM support says about what to do with the results:
             Note that most will have multiple matches, but you may only be interested in the
             "one" record that is most appropriate.  In most cases they should be sorted properly,
             but not always (meaning the first ID from PC Compound is likely the one you want).
       Yikes.  I think that there may be a way to poke harder by looking at the mesh label and seeing if it's in the
       synonyms for the pubchem compound?  Looking at some of these, it looks like one way to get multiple pubchem
       cids is that the same mesh will map to different stereoisomers, and also different salt forms (or unsalted).
       The other (worse) thing that can happen is when there is a mesh term that is a higher level term like
       "Calcium Channel Agonists".  Then we get a pile of CIDs back, and none of them really map to the concept, but are
       instances of the concept.  I think that we'll put in a threshold. If we only see a couple or 3, we use them all,
       if we see more than that, we give up.
    4. There's actually another thing that can come back: EC numbers. These are useful, in that they are clean.
       But they're identifiers for enzymes.  Yes, an enzyme is a chemical_substance too, but it's not really what
       we're trying to do here.  Nevertheless, let's hang onto them. We dump them and then if we want to handle
       later we can. There are about 10000 that come back with EC...
       EC:   10000

       Of course, the usefulness of these approaches is inverse with the frequency of their occurence:
       UNII: 14545
       CAS:  60880
       0:    190966"""
    unmapped_mesh, term_to_cas, term_to_unii, term_to_EC, labels = parse_mesh(pull('ftp.nlm.nih.gov','/online/mesh/rdf', 'mesh.nt.gz'))
    #This is just a way to cache some slow work so you can come back to it dig around without re-running things.
    umfname = os.path.join(os.path.dirname (__file__), 'unmapped.pickle')
    mcfname = os.path.join(os.path.dirname (__file__), 'meshcas.pickle')
    mufname = os.path.join(os.path.dirname (__file__), 'meshunii.pickle')
    ecfname = os.path.join(os.path.dirname (__file__), 'meschec.pickle')
    labelname = os.path.join(os.path.dirname(__file__), 'meshlabels.pickle')
    with open(umfname,'wb') as um, open(mcfname,'wb') as mc, open(mufname,'wb') as mu, open(ecfname,'wb') as mec, open(labelname,'wb') as ml:
        pickle.dump(unmapped_mesh,um)
        pickle.dump(term_to_cas,mc)
        pickle.dump(term_to_unii,mu)
        pickle.dump(term_to_EC,mec)
        pickle.dump(labels,ml)
    '''
    with open(umfname,'rb') as um, open(mcfname,'rb') as mc, open(mufname,'rb') as mu, open(ecfname,'rb') as mec:
        unmapped_mesh=pickle.load(um)
        term_to_cas=pickle.load(mc)
        term_to_unii=pickle.load(mu)
        term_to_EC=pickle.load(mec)
    '''
    #mesh_to_unii is one of the files read by chemicals.py
    muni_name = os.path.join(os.path.dirname(__file__), 'mesh_to_unii.txt')
    mec_name = os.path.join(os.path.dirname(__file__), 'mesh_to_EC.txt')
    dump(term_to_unii,muni_name)
    dump(term_to_EC,mec_name)
    context = rosetta.service_context
    try:
        api_key = context.config['EUTILS_API_KEY']
    except:
        api_key = None

    term_to_pubchem_by_mesh = lookup_by_mesh(unmapped_mesh,api_key)
    term_to_pubchem_by_cas = lookup_by_cas(term_to_cas,api_key)

    term_to_pubchem = {**term_to_pubchem_by_cas, **term_to_pubchem_by_mesh}
    #mesh_to_pubchem is one of the files that chemicals.py is looking for.
    mpc_name = os.path.join(os.path.dirname(__file__), 'mesh_to_pubchem.txt')
    dump(term_to_pubchem,mpc_name)

