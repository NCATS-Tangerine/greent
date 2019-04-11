#lives on stars-c8 //mnt/sdd1/bizon/pairs
import os

def extract_pubmed(url):
    return url.split('/')[-1][:-1]

def make_curie(url):
    if 'hp-logical-definitions-subq' in url:
        return None,None
    if '#' in url:
        return None,None
    obo = '<http://purl.obolibrary.org/obo/'
    oboext = '<http://purl.obolibrary.org/obo/uberon/ext.owl/'
    mesh = '<http://id.nlm.nih.gov/mesh/'
    hgnc = '<http://identifiers.org/hgnc/'
    dcel = '<http://purl.org/dc/elements/1.1/'
    dcterms = '<http://purl.org/dc/terms/'
    foaf = '<http://xmlns.com/foaf/0.1/'
    if url.startswith(oboext):
        goodpart=url[len(oboext):-1]
        return goodpart.split('_')
    elif url.startswith(obo):
        goodpart=url[len(obo):-1]
        return goodpart.split('_')
    elif url.startswith(mesh):
        goodpart=url[len(mesh):-1]
        return 'MeSH',goodpart
    elif url.startswith(hgnc):
        goodpart = url[len(hgnc):-1]
        return goodpart.split(':')
    elif url.startswith(dcel):
        goodpart = url[len(dcel):-1]
        return 'dcelements',goodpart
    elif url.startswith(dcterms):
        goodpart = url[len(dcterms):-1]
        return 'dcterms',goodpart
    elif url.startswith(foaf):
        goodpart = url[len(foaf):-1]
        return 'foaf',goodpart
    print(url)
    return None, url


outfiles = {}

with open('twocols.txt','w') as outf:
    tdir = '../../balhoff/ttl'
    ttls = os.listdir(tdir)
    for ttl in ttls:
        with open('{}/{}'.format(tdir,ttl),'r') as inf:
            for line in inf:
                if line.startswith('<https://www.ncbi.nlm.nih.gov/pubmed/'):
                    pubmed_id = extract_pubmed(line.strip())
                else:
                    sline = line.strip()
                    if len(sline) == 0:
                        continue
                    parts = sline.split()
                    uri = parts[1]
                    prefix,ident = make_curie(uri)
                    if prefix is None:
                        continue
                    if prefix not in outfiles:
                        outf = file('omnicorp/{}'.format(prefix),'w')
                        outfiles[prefix] = outf
                    curie = '{}:{}'.format(prefix,ident)
                    outfiles[prefix].write('{}\t{}\n'.format(pubmed_id,curie))

for outf in outfiles.values():
    outf.close()
